# This python code is to monitor AWS S3 bucket and report if there are any new object created in the bucket.
# Pre-requisite is to have an AWS accoount with S3 bucket created.
# This module has the code to create an AWS SQS. Configure the bucket to send notification messages to the SQS queue
# created. Consume the message and parse only the message which has the eventName as ObjectCreated:Put, and extract 
# S3 key containing the file. The files will be verified if it is CSV then will be sent to the Spark cluster. 

import os
import boto3
import sys
import string
import random
import gzip
import time
import json
import urllib
from pathlib import Path
from boto3.session import session

#Creating configurations to connect to AWS CLI

bucket_name = ''
client_options = {
	'aws_access_key_id' : '',
	'aws_secret_access_key' : '',
	'aws_session_token' : '',
	'region_name' : ''
}

s3_client = boto3.client('s3', **client_options)
sqs_client = boto3.client('sqs', **client_options)

#Creating SQS Queue:

queue_name = 's3-bucket-monitor-'+'.'.join(random.choice(string.ascii_lowercase) for i in range(10))
queue_url = sqs_client.create_queue(QueueName=queue_name)['QueueUrl']
time.sleep(1)
queue_arn = sqs_client.get_queue_attributes(QueueUrl = queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
sqs_policy = {
	"version" : "2023-02-03",
	"Id" : "example-ID",
	"statement" : [
		{
			"Sid" : "Monitor-SQS-ID",
			"Effect" : "Allow",
			"Principal" : {
			"AWS" : "" # Provide the account ID
			},
			"Action" : [
				"SQS:SendMessage"
				],
			"Resource" : queue_arn,
			"Condition" : {
				"ArnLike" {
					"aws:SourceArn": f"arn:aws:s3:*:*:{bucket_name}"
				},
			}
		}
	]
}

sqs_client.set_queue_attributes(QueueUrl = queue_url, Attributes = {
	"Policy" : json.dumps(sqs_policy)
	}
)

#Configuring the bucket:

bucket_notification_config = {
    'QueueConfigurations': [
        {
            'QueueArn': queue_arn,
            'Events': [
                's3:ObjectCreated:*',
            ]
        }
    ],
}

s3_client.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration=bucket_notification_config
)


#Consuming the messages in the queue:

csv_files = []

while True:
	resp = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
        )

	if 'Messages' not in resp:
        print('No messages found')
        continue

    for message in resp['Messages']:
        body = json.loads(message['Body'])
        #Sample message output is attached in the file named S3-resopnse.txt

        try:
            record = body['Records'][0]
            event_name = record['eventName']
        except Exception as e:
            print(f'Ignoring {message=} because of {str(e)}')
            continue

        if event_name.startswith('ObjectCreated'):
            # New object is created in the bcket and we need to verify if it is a CSV file and append to the list csv_files
            s3_info = record['s3']
            object_info = s3_info['object']
            key = urllib.parse.unquote_plus(object_info['key'])
            #print(f'Found new object {key}')
            csv_files.append(key)

