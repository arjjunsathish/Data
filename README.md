# Project

## SQL Queries:

1. Created the Schema for the MSFT data set that was provided in the CSV. The data types used are int for the primary key, VARCHAR for the stock name, and the rest of the columns were defined as Float and integer. 

2. Second query to pull the average of the High, Low, Open, Close, Adj Close and volume was computed using the Union statement. Alternate method to calculate the average using Grouping Sets is also given. Grouping sets will be helpful when the data is huge and it avoids of repeated scaning unlike the union statement. 

**Data Pipeline Design to read continious Stream of Stock Data:**

Below is the architecture showing the different technologies used to design the pipeline. Detailed explanation to choose the components are discussed below:

<img width="1212" alt="image" src="https://user-images.githubusercontent.com/66326573/216806510-1b5226b5-e33e-4ede-bf0c-6b310d903a7a.png">


**Step1: To read the data from the Data Source**. 

The data source chosen here is S3 (AWS). It is an object storage system and its more efficient to read objects unlike other storage systems like Ceph (though objedct store). Also, it is one of the most used storage in the industries. 

SQS - AWS Simple Queue System is used to notify any new addition of objects in the S3 bucket. The configuration of the SQS policy is given below: 

**sqs_policy = {
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
}**


**Pre-requisites: **


1. AWS account with IAM 
2. Region
3. ARN
4. S3 bucket configured. 

Register the bucket to the SQS configured. 

**bucket_notification_config = {
    'QueueConfigurations': [
        {
            'QueueArn': queue_arn,
            'Events': [
                's3:ObjectCreated:*',
            ]
        }
    ],
}**

This code runs in an infinite loop to constantly monitor the queue created. Whenever there is a new file uploaded to the S3 bucket, a message is written to the queue. The python code reads the messages in the queue and examines if there is a key word like "ObjectCreated" present in the messages. If it is present then it does further parsing to pull the object that was added. It also verifies if the object is a CSV file or some random object. **A sample response is shown in the file names S3-response.txt, it is a JSON object.**

**There are several ways of doing it but this method was chosen as the SQS will notify the addition of new objects to any directory level in the S3 bucket. Other methods like constantly pulling the directory contents and checking the difference between the files was alos considered but it is less efficient when compared to this mechanism. **

The output of the module **Monitor-S3.py** is a list of CSV files. This is will also ensure that the same file is not read again and again. 

**Step 2: Kafka Ecosystem:**

The list of CSV files are fed as input to the **Kafka Producer**. This module is written assuming a single node Kafka ecosystem, however the same module can be replicated to a distributed architecture. The list of CSV files are iterated, read and sent to the Kafka Producer. Kafka producer serialises the JSON object and sends to the topic configured "msft_data" 

**kafka-topics --bootstrap-server 127.0.0.1:9092 --topic msft_data --create --partitions 1 --replication-factor 1**

**producer = Kafkaproducer(bootstrap_servers=['<ipaddress of kafka>:9092'], value_serializer = json_serializer)**

**Kafka was chosen because, it ensures continious, real time streaming and the consumer can read the missed data using the offset. Hence the data is not missed.**

Step 1 & Step 2 can be combined into one module, but for ease of understanding I have made it into seperate modules. 
	
**Step 3: Spark Cluster - PySpark:**

The consumer that reads the data from Kafka is Spark (using PySpark). The incomming data is a serialised JSON stream. Following steps are performed before calculating the rolling average for 20, 50 and 200 days. 

1. Spark Session is created.
	
	**spark = SparkSession\
        .builder\
        .master('local')\
        .appName("MSFT")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
        .getOrCreate()**

2. Schema is defined: 
	
	**schema1 = StructType([StructField('Date', TimestampType(), True),
			StructField ('StockName', StringType(), True), 
			StructField ('Open', DoubleType(), True), 
			StructField ('High', DoubleType(), True), 
			StructField ('Low', DoubleType(), True), 
			StructField ('Close', DoubleType(), True), 
			StructField ('AdjClose', DoubleType(), True), 
			StructField ('Volume', DoubleType(), True)])**

3. The incoming data stream is read Kafka
	
	**input_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "<ipaddress of kafka>:9092")\
        .option("subscribe", "msft_data")\
        .option("startingOffsets", "earliest")\
        .load()**
	
4. The stream is converted to a Dataframe
	
	**value_df = input_df.select(from_json(col("value").cast("string"),schema).alias("value"))**
	
5. The Dataframe data types are converted from String to double to calculate the rolling average.
	
	**mstf_df = value_df.withColumn("Date", col("Date").cast(DateType()))\
        					.withColumn("StockName", col("Open").cast(DoubleType()))\
        					.withColumn("Open", col("Open").cast(DoubleType()))\
        					.withColumn("High", col("High").cast(DoubleType()))\
        					.withColumn("Low", col("Low").cast(DoubleType()))\
        					.withColumn("Close", col("Close").cast(DoubleType()))\
        					.withColumn("AdjClose", col("AdjClose").cast(DoubleType()))\
        					.withColumn("Volume", col("Volume").cast(DoubleType()))**
	
6. Data Cleaning to remove the NULL and NA values are performed. 
	
**Step 4: Creating a rolling window and writnig to DB:**

1. Before calculating the rolling average, we create a sliding window function that can slide over the Dataframe and calculate the average. 
	
	**wma_20 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-20, 0)
        wma_50 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-50, 0)
        wma_200 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-200, 0)**

2. Calculation of average: 
	
	**df_20days = df2.withColumn('20DaysAvg', avg("Open").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("High").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("Low").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("Close").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("AdjClose").over(wma_20))\	
        			   .withColumn('20DaysAvg', avg("Volume").over(wma_20))**
	
It is also to be noted that the rolling average should not be calculated for the first 19 days, for the 20 days. rolling average. Hence we write the below code. 
	**df_20days = df_20days.withColumn("mid", monotonically_increasing_id()) \
                             .withColumn("20DaysAvg", when(col("mid")<= 19, lit(None)).otherwise(col("20DaysAvg"))) \
                             .drop("mid")**

3. Persist the values to the DB: 
	**df_20days.write.mode("append").format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",user="tutorial_user",password="user_password",
        url="jdbc:mysql://mysql.dbmstutorials.com:3306?serverTimezone=UTC&useSSL=false", dbtable="Thales.20DaysRollingAverage").save()**
									  
**Spark (PySpark) was chosen because Spark provides real time data processing and more over there are 2 steps in the data processing. Transformation and Action. This will aloow us to debug more efficiently and also not load the dataframe and hang the system.**
									  
									  
**Step 5: Pulling data using API:**

In order to pull the data using an API I have chosen a Microservice. **This is because, microservice allows to decouple the data pipeline from the Machine Learning models consuming the data pipelines. Also, if we want to deploy 2 or multiple different versions of ML model, we can deploy only the model and use the same pipeling if there exists a Microservice. This is ease the deployment process and also helps in a comparitive study.** The API design code in provided in the file msft_api.py.

Please note the above design was done on a single node system, however the design can be extended to a distributed architecture in which we can invovle deployment rules like Falut Tolerance, Scalability. 									  


