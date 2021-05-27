from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redis_schema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr", BooleanType()),
    StructField("zSetEntries", ArrayType(
        StructType([
            StructField("element", StringType()),
            StructField("Score", StringType())
        ])
    ))
])


# create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType())
])

# create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
stedi_schema = StructType([
    StructField("customer", StringType() ),
    StructField("score", StringType() ),
    StructField("riskDate", StringType() )
])

# create a spark application object
spark = SparkSession.builder.appName('stedi-risk').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
redis_raw_stream = spark.readStream.format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("startingOffsets", "earliest")\
    .option("subscribe","redis-server")\
    .load()

# cast the value column in the streaming dataframe as a STRING 
redis_stream = redis_raw_stream.selectExpr("cast(key as string) as key", "cast(value as string) as value")

# parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet

redis_deserialized = redis_stream.withColumn( "value", from_json( "value", redis_schema ) ).select(col("value.*"))

redis_deserialized.createOrReplaceTempView( "RedisSortedSet" )

# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column

redis_sttmt = spark.sql("select key , zSetEntries[0].element as encodedCustomer from RedisSortedSet ")

# take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+
# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

redis_unbase64 = redis_sttmt.select( unbase64( redis_sttmt.encodedCustomer ).cast("string").alias("redisEvent") )\
                            .filter( col("redisEvent").contains("email") )

customer_deserialized = redis_unbase64.withColumn("customer", from_json( "redisEvent", customer_schema )).select(col("customer.*"))\
                            .filter( col("email").isNotNull() )

#customer_deserialized.writeStream.format('console').outputMode('append').start().awaitTermination() ## DEBUG

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords

customer_deserialized.createOrReplaceTempView( "CustomerRecords" )

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql( "select customerName, email, phone, birthDay from CustomerRecords where email is not null and birthDay is not null" )

# Split the birth year as a separate field from the birthday
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select( "email", split( emailAndBirthDayStreamingDF.birthDay, "-" ).getItem(0).alias("birthYear") )

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
stedi_raw_stream = spark.readStream.format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("startingOffsets", "earliest")\
    .option("subscribe","stedi-events")\
    .load()

# cast the value column in the streaming dataframe as a STRING 
stedi_stream = stedi_raw_stream.selectExpr("cast(key as string) as key", "cast(value as string) as value")

# parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
stedi_deserialized = stedi_stream.withColumn("value", from_json("value", stedi_schema) ).select(col("value.*"))

stedi_deserialized.createOrReplaceTempView("CustomerRisk")

# execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql( "select customer, score from CustomerRisk" )

# customerRiskStreamingDF.writeStream.format('console').outputMode('append').start().awaitTermination() ## DEBUG

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

join_dfs = customerRiskStreamingDF.join( emailAndBirthYearStreamingDF , expr("customer = email") )

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"} 
join_dfs.selectExpr( 
        "cast(email as string) as key", 
        "to_json(struct(*)) as value" 
    )\
    .writeStream.format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "score-birthyear-stream")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start().awaitTermination()


# /data/spark/sbin/start-master.sh
# tail -f /opt/spark-2.3.4-bin-hadoop2.7/logs/spark--org.apache.spark.deploy.master.Master-1-82d2ffcf02e1.out

# /data/spark/sbin/start-slave.sh [sparkmasterhost:port]
# /data/spark/sbin/start-slave.sh spark://82d2ffcf02e1:7077

# tail -f /opt/spark-2.3.4-bin-hadoop2.7/logs/spark--org.apache.spark.deploy.worker.Worker-1-82d2ffcf02e1.out