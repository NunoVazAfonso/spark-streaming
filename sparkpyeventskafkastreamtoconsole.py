from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

customer_schema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", StringType()),
])

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
spark = SparkSession.builder.appName("stedi-reader").getOrCreate()                        

spark.sparkContext.setLogLevel('WARN')

stedi_raw_stream = spark.readStream.format('kafka')\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("startingOffsets", "earliest")\
    .option("subscribe", "stedi-events")\
    .load()

stedi_stream = stedi_raw_stream.selectExpr("cast(key as string) as key", "cast(value as string) as value")

# parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
# storing them in a temporary view called CustomerRisk
stedi_deserialized = stedi_stream.withColumn("value", from_json("value", customer_schema)).select(col("value.*"))

stedi_deserialized.createOrReplaceTempView("CustomerRisk")

# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 

customerRiskStreamingDF.writeStream.format('console').outputMode('append').start().awaitTermination()