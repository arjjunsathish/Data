#This module will receive the newly added CSV file from S3 bucket. The output of the module "monitor-S3.py" will be consumed by this modle. It will read the CSV files and convert the rows to a json stream. The json stream will be 
#serialised and sent for consumption by the Spark Cluster. This is the structured 

import json
import csv
import time
import os
import gzip
import codecs
import pandas as pandas
import numpy as np
import sys
from kafka import KafkaProducer
from collections import Counter

#print (sys.argv[1])

def convert_csv_to_dict(file1):
	with open(file1) as f:
		reader = csv.DictReader(f)
		for row in reader:
			#print (row)
			return row


def json_serializer(data):
	return json.dumps(data)


producer = Kafkaproducer(bootstrap_servers=['<ipaddress of kafka>:9092'], value_serializer = json_serializer)


if __name__ == "__main__":
	while 1 == 1:
		for x in range(len(file)):
			file = file_list[x]
			msft_data = convert_csv_to_dict(file)
			#print (msft_data)
			producer.send('msft_data', msft_data)

		

