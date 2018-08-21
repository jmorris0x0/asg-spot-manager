import os
import boto3
import datetime
import base64
import time
import pprint
import json
import re
import tokenize
import token
from botocore.exceptions import ClientError

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class ASG_spot_manager():
    def __init__(self, region, tag, lookback, lc_switching_delay):
        self.asgTag = tag
        self.region = region
        self.lookback = lookback  # seconds
        self.lc_switching_delay = lc_switching_delay  # seconds
        self.aws_od_endpoint = 'http://a0.awsstatic.com/pricing/1/ec2/linux-od.min.js'
        self.ec2_client = boto3.client('ec2', region_name=self.region)
        self.autoscale_client = boto3.client('autoscaling',
                                             region_name=self.region)

        self.denial_statuses = ['capacity-not-available',
                                'capacity-oversubscribed',
                                'instance-terminated-by-price',
                                'instance-terminated-capacity-oversubscribed',
                                'instance-terminated-no-capacity',
                                'price-too-low']

        self.pp = pprint.PrettyPrinter(indent=4)

    def run(self):
        target_asgs = self.build_tagged_ASG_list()

        if not len(target_asgs):
            print("No groups found with " + self.asgTag + " tag. Exiting.")
            return
        for asg in target_asgs:
            asg_name = asg['AutoScalingGroupName']
            self.manage_group(asg)

    def manage_group(self, asg_dict):
        asg_name = asg_dict['AutoScalingGroupName']
        print("Managing ASG: {}".format(asg_name))
        # Is the ASG currently spot or on demand?
        is_spot = self.is_asg_currently_spot(asg_dict)

        if is_spot:
            print("ASG is currently set to spot.")
            # Have we been outbid or terminated during the lookback period?
            spot_failure = self.check_spot_requests(asg_name)

            if spot_failure:
                print("Spot bid failure or termination found for ASG during lookback.")
                self.switch_to_on_demand(asg_dict)
                return
            else:
                print("No spot bid failures or terminations found for ASG during lookback.")
                print("No action required for this group.")
                return
        else:
            print("ASG is currently set to on-demand.")

            if self.is_spot_greater_than_on_demand(asg_dict):
                print("Instance spot price is currently greater than instance on-demand price.")
                print("No action required for this group.")
                return

            elif self.time_since_lc_update(asg_dict) > self.lc_switching_delay:
                print("Instance spot price is currently less than instance on-demand price.")
                print("Time since last launch config edit is greater than LC_SWITCHING_DELAY.")
                self.switch_to_spot(asg_dict)
                return
            else:
                print("No action required for this group.")

    def time_since_lc_update(self, asg_dict):
        now = datetime.datetime.now(datetime.timezone.utc)
        launch_config_name = asg_dict['LaunchConfigurationName']
        config = self.get_launch_config(launch_config_name)
        lc_created_at = config['CreatedTime']
        td_since_update = now - lc_created_at
        seconds_since_update = td_since_update.total_seconds()

        return seconds_since_update

    def is_spot_greater_than_on_demand(self, asg_dict):
        asg_zones = asg_dict['AvailabilityZones']
        launch_config_name = asg_dict['LaunchConfigurationName']
        config = self.get_launch_config(launch_config_name)
        asg_instance_type = config['InstanceType']
        spot_prices = self.get_zone_spot_prices(asg_instance_type, asg_zones)
        spot_zone_min = min(spot_prices)
        spot_zone_max = max(spot_prices)
        od_price = self.get_lc_on_demand_price(config)

        if spot_zone_min > float(od_price):
            return True
        else:
            return False

    def build_tagged_ASG_list(self):
        """
        Returns a list of Autoscaling groups that match the given tag.
        """
        groups = self.autoscale_client.describe_auto_scaling_groups()['AutoScalingGroups']
        tagged_groups = []
        for group in groups:
            tags = group['Tags']

            for tag in tags:
                key = tag['Key']
                value = tag['Value']
                if (key == self.asgTag) & (value == 'true'):
                    tagged_groups.append(group)
                    break

        return tagged_groups

    def is_asg_currently_spot(self, asg_dict):
        launch_config_name = asg_dict['LaunchConfigurationName']
        config = self.get_launch_config(launch_config_name)

        if 'SpotPrice' in config:
            return True
        else:
            return False

    def check_spot_requests(self, asg_name):
        """
        Given an autoscaling group name, returns True if there has been a
        spot request failure or spot termination in the lookback period.
        Otherwise returns False.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        requests = self.ec2_client.describe_spot_instance_requests()['SpotInstanceRequests']
        asg_requests = []
        for request in requests:
            tags = request.get('Tags')
            if tags:
                for tag in tags:
                    key = tag['Key']
                    value = tag['Value']
                    if (key == 'launched-for-asg') & (value == asg_name):
                        asg_requests.append(request)
                        break

        if not len(asg_requests):
            return False

        for request in asg_requests:
            status = request['Status']
            code = status['Code']
            update_time = status['UpdateTime']
            td_since_update = now - update_time
            seconds_since_update = td_since_update.total_seconds()

            if (seconds_since_update < self.lookback) & (code in self.denial_statuses):
                return True

        return False

    def get_launch_config(self, name):
        """
        """
        launch_configs = self.autoscale_client.describe_launch_configurations()['LaunchConfigurations']

        for launch_config in launch_configs:
            if launch_config['LaunchConfigurationName'] == name:
                return launch_config

        return False

    def switch_to_on_demand(self, asg_dict):
        """
        Copy the current LC while removing the SpotPrice key. Next switch to
        the new LC and delete the old one.
        """
        print("Switching ASG to on-demand instances.")
        launch_config_name = asg_dict['LaunchConfigurationName']
        config = self.get_launch_config(launch_config_name)

        # Remove empty values:
        config = {k: v for k, v in config.items() if v}

        # Change name:
        old_lc_name = config['LaunchConfigurationName']

        if old_lc_name.endswith('*'):
            new_lc_name = old_lc_name.strip('*')
        else:
            new_lc_name = old_lc_name + '*'

        config['LaunchConfigurationName'] = new_lc_name

        # Decode user data:
        config['UserData'] = base64.decodebytes(config['UserData'].encode()).decode()

        # Remove unused keys:
        config.pop('LaunchConfigurationARN', None)
        config.pop('CreatedTime', None)
        config.pop('SpotPrice', None)

        response = self.autoscale_client.create_launch_configuration(**config)
        time.sleep(2)

        # Make sure new launch config exists.
        response = self.get_launch_config(new_lc_name)

        if not response:
            print("Error setting new launch config.")
            return
        else:
            print("New launch config created.")

        # If the LC InstanceMonitoring is set to false, first disable the
        # collection of group metrics. Otherwise an error will occur:
        if (not config.get('InstanceMonitoring')['Enabled']):
            self.autoscale_client.disable_metrics_collection(
                AutoScalingGroupName=asg_dict['AutoScalingGroupName'])
        time.sleep(2)

        # Update ASG to use new LC:
        self.autoscale_client.update_auto_scaling_group(
            AutoScalingGroupName=asg_dict['AutoScalingGroupName'],
            LaunchConfigurationName=new_lc_name)
        time.sleep(2)

        # Delete old LC:
        self.autoscale_client.delete_launch_configuration(
            LaunchConfigurationName=old_lc_name)

    def switch_to_spot(self, asg_dict):
        """
        Copy the current LC while adding the SpotPrice key. Next switch to
        the new LC and delete the old one.
        """
        print("Switching ASG to spot instances.")
        launch_config_name = asg_dict['LaunchConfigurationName']
        config = self.get_launch_config(launch_config_name)

        # Get current on-demand price:
        od_price = self.get_lc_on_demand_price(config)

        print("Setting spot bid to current on-demand price of $" + od_price)

        # Remove empty values:
        config = {k: v for k, v in config.items() if v}

        # Change name:
        old_lc_name = config['LaunchConfigurationName']

        if old_lc_name.endswith('*'):
            new_lc_name = old_lc_name.strip('*')
        else:
            new_lc_name = old_lc_name + '*'

        config['LaunchConfigurationName'] = new_lc_name

        # Decode user data:
        config['UserData'] = base64.decodebytes(config['UserData'].encode()).decode()

        # Remove unused keys:
        config.pop('LaunchConfigurationARN', None)
        config.pop('CreatedTime', None)

        config['SpotPrice'] = od_price

        response = self.autoscale_client.create_launch_configuration(**config)
        time.sleep(2)

        # Make sure new launch config exists.
        response = self.get_launch_config(new_lc_name)

        if not response:
            print("Error setting new launch config.")
            return
        else:
            print("New launch config created.")

        # If the LC InstanceMonitoring is set to false, first disable the
        # collection of group metrics. Otherwise an error will occur:
        if (not config.get('InstanceMonitoring')['Enabled']):
            self.autoscale_client.disable_metrics_collection(
                AutoScalingGroupName=asg_dict['AutoScalingGroupName'])
        time.sleep(2)

        # Update ASG to use new LC:
        self.autoscale_client.update_auto_scaling_group(
            AutoScalingGroupName=asg_dict['AutoScalingGroupName'],
            LaunchConfigurationName=new_lc_name)
        time.sleep(2)

        # Delete old LC:
        self.autoscale_client.delete_launch_configuration(
            LaunchConfigurationName=old_lc_name)

    def get_lc_on_demand_price(self, lc_dict):
        instance_type = lc_dict['InstanceType']
        raw_price_data = self.load_data(self.aws_od_endpoint)
        on_demand_price = self.get_od_price_from_response(raw_price_data, self.region, instance_type)

        return on_demand_price

    def get_od_price_from_response(self, response, ASG_region, instance_type):
        regions = response['config']['regions']

        for region in regions:
            if region['region'] == ASG_region:
                types = region['instanceTypes']
                break
        if not types:
            print('Error: AWS region not found.')
            return False

        for type in types:
            sizes = type['sizes']
            for size in sizes:
                if size['size'] == instance_type:
                    od_price = size['valueColumns'][0]['prices']['USD']
                    break
            if not od_price:
                print('Error: instance price not found.')
                return False

        return od_price

    def load_data(self, url):
        f = urllib2.urlopen(url)
        request = f.read()

        if isinstance(request, bytes):
            request = request.decode('utf8')

        # strip initial comment (with newline)
        modified_request = re.sub(re.compile(r'/\*.*\*/\n', re.DOTALL), '', request)
        # strip from front of request
        modified_request = re.sub(r'^callback\(', '', modified_request)
        # strip from end of request
        modified_request = re.sub(r'\);*$', '', modified_request)

        modified_request = self.fixup_js_literal_with_comments(modified_request)
        obj = json.loads(modified_request)

        return obj

    def fixup_js_literal_with_comments(self, in_text):
        """
        Taken from:
        https://github.com/IT-corridor/cloud-pricing
        """
        result = []
        tokengen = tokenize.generate_tokens(StringIO(in_text).readline)

        sline_comment = False
        mline_comment = False
        last_token = ''

        for tokid, tokval, _, _, _ in tokengen:
            # ignore single line and multi line comments
            if sline_comment:
                if (tokid == token.NEWLINE) or (tokid == tokenize.NL):
                    sline_comment = False
                continue

            # ignore multi line comments
            if mline_comment:
                if (last_token == '*') and (tokval == '/'):
                    mline_comment = False
                last_token = tokval
                continue

            # fix unquoted strings
            if tokid == token.NAME:
                if tokval not in ['true', 'false', 'null', '-Infinity', 'Infinity', 'NaN']:
                    tokid = token.STRING
                    tokval = u'"%s"' % tokval

            # fix single-quoted strings
            elif tokid == token.STRING:
                if tokval.startswith("'"):
                    tokval = u'"%s"' % tokval[1:-1].replace('"', '\\"')

            # remove invalid commas
            elif (tokid == token.OP) and ((tokval == '}') or (tokval == ']')):
                if (len(result) > 0) and (result[-1][1] == ','):
                    result.pop()

            # detect single-line comments
            elif tokval == "//":
                sline_comment = True
                continue

            # detect multiline comments
            elif (last_token == '/') and (tokval == '*'):
                result.pop()  # remove previous token
                mline_comment = True
                continue

            result.append((tokid, tokval))
            last_token = tokval

        return tokenize.untokenize(result)

    def get_zone_spot_prices(self, instance_type, zones):
        price_list = []
        for zone in zones:
            try:
                price = self.ec2_client.describe_spot_price_history(
                    InstanceTypes=[instance_type],
                    MaxResults=1,
                    AvailabilityZone=zone,
                    ProductDescriptions=['Linux/UNIX (Amazon VPC)'])['SpotPriceHistory'][0]['SpotPrice']
                price_list.append(float(price))

            except ClientError as e:
                print(e.response['Error']['Message'])

        return price_list


def lambda_handler(event, context):
    region = os.environ['REGION']
    asg_tag = os.environ.get('ASG_TAG')
    lc_switching_delay = os.environ.get('LC_SWITCHING_DELAY')
    lc_switching_delay = float(lc_switching_delay)
    lookback = os.environ.get('LOOKBACK')
    lookback = float(lookback)

    spot_manager = ASG_spot_manager(region, asg_tag, lookback, lc_switching_delay)
    spot_manager.run()

# Remove this after done testing locally:
# if __name__ == "__main__":
#     lambda_handler(None, None)

# Run to prep running code locally:
# export REGION='us-west-2'
# export ASG_TAG='enable-spot-manager'
# export LOOKBACK=300
# export LC_SWITCHING_DELAY=3600

