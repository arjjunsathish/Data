#This module is using Python Pyspark library. It will consume the messages, sent by Kafka Producer and processes it. 


from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr, from_json, col, concat
from pyspark.sql.window import Window
from pyspark.sql import functiona as func

if __name__ == '__main__':

	#Creating a Spark session
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName("MSFT")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
        .getOrCreate()

        #Building Schema:
		schema1 = StructType([StructField('Date', TimestampType(), True),
			StructField ('StockName', StringType(), True), 
			StructField ('Open', DoubleType(), True), 
			StructField ('High', DoubleType(), True), 
			StructField ('Low', DoubleType(), True), 
			StructField ('Close', DoubleType(), True), 
			StructField ('AdjClose', DoubleType(), True), 
			StructField ('Volume', DoubleType(), True)])

		#Receiving data from Kafka. starting offset is set to earliest, as it will read all the messages from the queue. 
		input_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "<ipaddress of kafka>:9092")\
        .option("subscribe", "msft_data")\
        .option("startingOffsets", "earliest")\
        .load()

        #input_df.printSchema()

        #Transform the output to a Dataframe
        value_df = input_df.select(from_json(col("value").cast("string"),schema).alias("value")) 

        #Converting the values of the dataframe to integer for calculation of rolling average. 
        mstf_df = value_df.withColumn("Date", col("Date").cast(DateType()))\
        					.withColumn("StockName", col("Open").cast(DoubleType()))\
        					.withColumn("Open", col("Open").cast(DoubleType()))\
        					.withColumn("High", col("High").cast(DoubleType()))\
        					.withColumn("Low", col("Low").cast(DoubleType()))\
        					.withColumn("Close", col("Close").cast(DoubleType()))\
        					.withColumn("AdjClose", col("AdjClose").cast(DoubleType()))\
        					.withColumn("Volume", col("Volume").cast(DoubleType()))

        #Remove duplicate records and remove "na" values
        df1 = mstf_df.drop_duplicates()
        df2 = df1.na.drop()

        #Create window to calculate the rolling average.
        wma_20 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-20, 0)
        wma_50 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-50, 0)
        wma_200 = Window.partitionBy('StockName').orderBy('Date').rowsBetween(-200, 0)

        #Actual Calculation:
        df_20days = df2.withColumn('20DaysAvg', avg("Open").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("High").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("Low").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("Close").over(wma_20))\
        			   .withColumn('20DaysAvg', avg("AdjClose").over(wma_20))\	
        			   .withColumn('20DaysAvg', avg("Volume").over(wma_20))

        #The first 19 data points should not have a moving average, so we nullify them
        df_20days = df_20days.withColumn("mid", monotonically_increasing_id()) \
                             .withColumn("20DaysAvg", when(col("mid")<= 19, lit(None)).otherwise(col("20DaysAvg"))) \
                             .drop("mid")

        df_50days = df2.withColumn('50DaysAvg', avg("Open").over(wma_50))\
        			   .withColumn('50DaysAvg', avg("High").over(wma_50))\
        			   .withColumn('50DaysAvg', avg("Low").over(wma_50))\
        			   .withColumn('50DaysAvg', avg("Close").over(wma_50))\
        			   .withColumn('50DaysAvg', avg("AdjClose").over(wma_50))\	
        			   .withColumn('50DaysAvg', avg("Volume").over(wma_50))

        #The first 49 data points should not have a moving average, so we nullify them
        df_50days = df_50days.withColumn("mid", monotonically_increasing_id()) \
                             .withColumn("20DaysAvg", when(col("mid")<= 49, lit(None)).otherwise(col("50DaysAvg"))) \
                             .drop("mid")


        df_200days = df2.withColumn('200DaysAvg', avg("Open").over(wma_200))\
        			   .withColumn('200DaysAvg', avg("High").over(wma_200))\
        			   .withColumn('200DaysAvg', avg("Low").over(wma_200))\
        			   .withColumn('200aysAvg', avg("Close").over(wma_200))\
        			   .withColumn('200DaysAvg', avg("AdjClose").over(wma_200))\	
        			   .withColumn('200DaysAvg', avg("Volume").over(wma_200))

        #The first 199 data points should not have a moving average, so we nullify them
        df_200days = df_200days.withColumn("mid", monotonically_increasing_id()) \
                             .withColumn("200DaysAvg", when(col("mid")<= 199, lit(None)).otherwise(col("200DaysAvg"))) \
                             .drop("mid")


        #Persist the values to Mysql Database, writing it in append mode
        df_20days.write.mode("append").format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
        													  user="tutorial_user",
        													  password="user_password",
        													  url="jdbc:mysql://mysql.dbmstutorials.com:3306?serverTimezone=UTC&useSSL=false",
        													  dbtable="Thales.20DaysRollingAverage").save()

       	df_50days.write.mode("append").format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
        													  user="tutorial_user",
        													  password="user_password",
        													  url="jdbc:mysql://mysql.dbmstutorials.com:3306?serverTimezone=UTC&useSSL=false",
        													  dbtable="Thales.50DaysRollingAverage").save()



       	df_200days.write.mode("append").format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
        													  user="tutorial_user",
        													  password="user_password",
        													  url="jdbc:mysql://mysql.dbmstutorials.com:3306?serverTimezone=UTC&useSSL=false",
        													  dbtable="Thales.200DaysRollingAverage").save()








