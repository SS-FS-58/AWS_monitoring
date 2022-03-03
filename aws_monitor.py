import boto3
import pandas as pd
import time
# Get EC2 , S3, RDS Instances.
ec2 = boto3.client('ec2')
s3 = boto3.resource('s3')
rds = boto3.client('rds')
s3_client = boto3.client('s3')

# Get Session.
session = boto3.session.Session()

# Function To Get EC2 Status
def get_ec2_status(response):
    # Declare Variables
    privateipaddress, instance_ids, state_names, stopped_time, stopped_reason, lunchtime= [], [], [], [], [], []
    # From Responses Iterate Instances List
    for res in response['Reservations']:
        for ins in res['Instances']:
            # From Response Json set the variables
            privateipaddress.append(ins['PrivateIpAddress'])
            instance_ids.append(ins['InstanceId'])
            stateofins = ins['State']['Name']
            state_names.append(stateofins)
            lunchtime.append(ins['LaunchTime'])
            # If the Instance is stopped
            if stateofins == 'stopped':
                stop_time = ins['StateTransitionReason'] # Get StateTransitionReason
                # Get stop time from StateTransitionReason
                stop_time = stop_time.split("(")[1]
                stop_time = stop_time.split(")")[0]
                stopped_time.append(stop_time)
                stopped_reason.append(ins["StateReason"]["Message"])
                # If this instance is down, upload a black file called '<instanseName>_down.txt'
                s3.Bucket('sl-sre-instance-state-test').put_object(Key='EC2/' + ins['InstanceId'] +'_down.txt')
            # If the Instance is running
            else:
                stopped_time.append("")
                stopped_reason.append("")
                # If an instance comes back up, the staus file shoould be removed
                obj = s3.Object("sl-sre-instance-state-test", 'EC2/' + ins['InstanceId'] +'_down.txt')
                obj.delete()
    # Return Pandas Format virable pd.
    return pd.DataFrame({
        'InstanceId': instance_ids,
        'PrivateIpAddress': privateipaddress,
        'State': state_names,
        'LunchTime': lunchtime,
        'StoppedTime': stopped_time,
        'StoppedReason': stopped_reason
    })

#Function to get DBS Status
def get_dbs_status(response):
    # Declare variables
    endpoint, instance_name, state_names, freestoragespace, stopped_time, stopped_reason, lunchtime = [], [], [], [], [], [], []
    for ins in response['DBInstances']: # Iterate DBInstances
        endpoint.append(ins['Endpoint']['Address']) # Get Endoint value.
        instance_name.append(ins['DBInstanceIdentifier']) # Get DBInstance Identifier.
        stateofins = ins['DBInstanceStatus'] # Get DBInstance Status.
        state_names.append(stateofins)
        lunchtime.append(ins['InstanceCreateTime']) # Get the created time of Instance 
        freestoragespace.append("free")
        # If the Instance is running
        if stateofins != 'available':
            stop_time = ins['LatestRestorableTime']
            stopped_time.append(stop_time)
            # If this instance is down, upload a black file called '<instanseName>_down.txt'
            s3.Bucket('sl-sre-instance-state-test').put_object(Key='RDS/' + ins['DBInstanceIdentifier'] + '_down.txt')
        else:
            stopped_time.append("")
            # If an instance comes back up, the staus file shoould be removed
            obj = s3.Object("sl-sre-instance-state-test",'RDS/' + ins['DBInstanceIdentifier'] + '_down.txt')
            obj.delete()
    # Return Pandas Format virable pd.
    return pd.DataFrame({
        'DBInstanceIdentifier': instance_name,
        'Endpoint': endpoint,
        'RunState': state_names,
        'LunchTime': lunchtime,
        'StoppedTime': stopped_time,
    })

# Create S3 Bucket and Display Bucket lists
response = s3_client.list_buckets()
bucket_exit = False
# Get Buckets Information
for bucket in response['Buckets']:
    print(f'BucketName:  {bucket["Name"]}')
    if bucket["Name"] == 'self-sec-challenge-20-05':
        bucket_exit = True
if not bucket_exit: # if Already the bucket don't exit.
    s3.create_bucket(Bucket='self-sec-challenge-20-05', CreateBucketConfiguration={'LocationConstraint': session.region_name})


# Polls every minute EC2 and RDS
while True:
    response = ec2.describe_instances()
    data_ec2 = get_ec2_status(response)
    # Order by EC2 Instance lunch time
    data_ec2 = data_ec2.sort_values(by='LunchTime')
    # Display EC2 status data
    print(data_ec2)
    dbs = rds.describe_db_instances()
    data_rds = get_dbs_status(dbs)
    # Order by RDS Instance lunch time.
    data_rds = data_rds.sort_values(by='LunchTime')
    # Display RDS status data
    print(data_rds)
    # Delay 1 minute.
    time.sleep(2)



