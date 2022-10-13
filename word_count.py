from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("word_counter") \
    .config('spark.jars.packages', 'com.arangodb:arangodb-spark-datasource-3.2_2.12:1.4.0') \
    .getOrCreate()    
    

inputPath = "./text.txt"
schema1 = StructType([StructField('word', StringType(), True)])    

print('step1')
inputDF = spark.read.option("delimiter", "/t").schema(schema1).csv(inputPath)
inputDF.show()

print('step2')
inputDF = inputDF.select(split(col("word")," ").alias("word"))
inputDF.show()

print('step3')
inputDF = inputDF.select(inputDF.word,explode(inputDF.word)).withColumnRenamed("col","unique_word")
inputDF.show()  

print('step4')
inputDF = inputDF.filter(inputDF["unique_word"] != "")
inputDF.show()  

print('step5')
inputDF = inputDF.groupBy("unique_word").count()    
inputDF.show()



# options = Map(
# 	"endpoints" -> "localhost:8001",
# 	"database" -> "word_count_db"
# )
 
# writeOptions = Map(
# 	"table.shards" -> "9",
# 	"overwriteMode" -> "replace",
# 	"table" -> "documents_db"

inputDF.write \
    .mode("Append") \
	.format("com.arangodb.spark") \
	.option("endpoints", "localhost:8001") \
	.option("database", "word_count_db") \
    .option("table", "documents_db") \
    .save()  



outputDF = spark.read \
	.format("com.arangodb.spark") \
	.option("endpoints", "localhost:8001") \
	.option("database", "word_count_db") \
    .option("table", "documents_db") \
    .load()  

outputDF.show()    

