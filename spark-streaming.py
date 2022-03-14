# System dependencies for CDH
import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")
# Importing required function
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as Func
# Utility functions definition
# Utility function for checking if Order is of type "ORDER"
def get_is_order(order_type):
    if order_type == "ORDER":
        return(1)
    else:
        return(0)
# Utility function for checking if Order is of type "RETURN"    
def get_is_return(order_type):
    if order_type == "RETURN":
        return(1) 
    else:
        return(0)
# Utility function for calculating total cost of order positive values denotes Order type and negative values denotes Return Type
def get_total_cost(order_type,items):
    
    total_cost = 0
    for item in items:
            total_cost=total_cost+(item['quantity']*item['unit_price'])
    if order_type == "ORDER":
        
        return (total_cost)
    else:
         return ((total_cost) * -1)
# Utility function for calculating total number of items present in a single order    
def get_total_item_count(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count
   
# Initialize Spark session
spark=SparkSession \
    .builder \
        .appName("OrderAnalyzer") \
            .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')  

# Reading Input from kafka 
orderRaw = spark \
    .readStream \
        .format("kafka") \
            .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
                .option("subscribe","real-time-project")  \
                    .option("failOnDataLoss","false") \
                      .option("startingOffsets", "latest")  \
                        .load() 
# Define schema of a single order

jsonSchema =  StructType([
                StructField("country", StringType()),
                StructField("invoice_no", LongType()) ,
                StructField("items", ArrayType(
                    StructType([
                        StructField("SKU", StringType()),
                        StructField("title", StringType()),
                        StructField("unit_price", FloatType()),
                        StructField("quantity", IntegerType())
                              ])
                        )),
                StructField("timestamp", TimestampType()),
                StructField("type", StringType()),  
            ])
            
CastedStream = orderRaw.select(from_json(col("value").cast("string"), jsonSchema).alias("data"))

orderStream = CastedStream.select("data.*")


# Defining the UDFs with the utility functions and Calculate additional columns
Total_cost = udf(get_total_cost, FloatType())

orderStream = orderStream \
               .withColumn("total_Cost", Total_cost(orderStream.type,orderStream.items))

isOrderType = udf(get_is_order, IntegerType())

orderStream = orderStream \
              .withColumn("is_Order", isOrderType(orderStream.type))

isReturnType = udf(get_is_return, IntegerType())

orderStream = orderStream \
               .withColumn("is_Return", isReturnType(orderStream.type))

add_total_item_count = udf(get_total_item_count, IntegerType())

orderStream = orderStream \
                   .withColumn("total_items", add_total_item_count(orderStream.items)) \

# expandedOrderStream contains all the columns for printing to console
expandedOrderStream = orderStream.select("invoice_no","country","timestamp","total_Cost","total_items","is_Order","is_Return")



# Calculate time based KPIs
aggStreamByTime = expandedOrderStream.withWatermark("timestamp", "10 minutes") \
			.groupby(window("timestamp","1 minute")) \
			.agg(sum("total_Cost").alias("Total_sales_volume"), \
			Func.approx_count_distinct("invoice_no").alias("OPM"), \
			sum("is_Order").alias("total_Order"), \
			sum("is_Return").alias("total_return"), \
			sum("total_items").alias("total_items"))

#calculate rate_of_return
aggStreamByTime = aggStreamByTime.withColumn("rate_of_return",aggStreamByTime.total_return/(aggStreamByTime.total_Order+aggStreamByTime.total_return))	

#calculate Average_Transaction_size
aggStreamByTime = aggStreamByTime.withColumn("Average_Transaction_size",aggStreamByTime.Total_sales_volume/(aggStreamByTime.total_Order+aggStreamByTime.total_return))

#select timebased data
aggStreamByTime = aggStreamByTime.select("window","OPM","Total_sales_volume","Average_Transaction_size","rate_of_return")	

# Calculate time and country based KPIs
aggStreamByTimeCountry = expandedOrderStream.withWatermark("timestamp", "10 minutes") \
			.groupby(window("timestamp","1 minute"),"country") \
			.agg(sum("total_Cost").alias("Total_sales_volume"), \
			Func.approx_count_distinct("invoice_no").alias("OPM"), \
			sum("invoice_no").alias("sum_invoice"), \
			sum("is_Order").alias("total_Order"), \
			sum("is_Return").alias("total_return"), \
			sum("total_items").alias("total_items"))

#rate_of_return
aggStreamByTimeCountry = aggStreamByTimeCountry.withColumn("rate_of_return",aggStreamByTimeCountry.total_return/(aggStreamByTimeCountry.total_Order+aggStreamByTimeCountry.total_return))

#select time and country based data
aggStreamFinal = aggStreamByTimeCountry.select("window","country","OPM","Total_sales_volume","rate_of_return")	

#print data with additional columns to console
queryConsole = expandedOrderStream \
       .select("invoice_no", "country", "timestamp","total_Cost", "total_items","is_Order","is_Return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()
#print timebased KPI's to hdfs
queryByTime = aggStreamByTime \
             .writeStream \
             .format("json") \
             .outputMode("append") \
             .option("truncate", "false") \
             .option("path","/user/ec2-user/time_KPI") \
             .option("checkpointLocation", "/user/ec2-user/time_KPI") \
             .trigger(processingTime="1 minute") \
             .start()
#print time and country based KPI's to hdfs    
queryFinal = aggStreamFinal \
            .writeStream \
            .format("json") \
            .outputMode("append") \
            .option("truncate", "false") \
            .option("path","/user/ec2-user/time_country_KPI") \
            .option("checkpointLocation", "/user/ec2-user/time_country_KPI") \
            .trigger(processingTime="1 minute") \
            .start()
    
queryConsole.awaitTermination()
queryByTime.awaitTermination()
queryFinal.awaitTermination()


