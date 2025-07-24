import os
import sys

from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

if __name__ == '__main__':
    schema = StructType().add("id", IntegerType()).add("action", StringType())
    users_schema = StructType().add("id", IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

    spark = SparkSession \
        .builder \
        .appName("ProduceConsoleApp") \
        .getOrCreate()

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("subscribe", "netology")
              .option("startingOffsets", "earliest")
              .load()
              )
    #печать схемы - необязательна
    source.printSchema()
    #работа над получаемыми DataFrame

    json_stream = source.select(col("timestamp").cast("string"),
                                from_json(col("value").cast("string"), schema).alias("parsed_value"))
    # проверим что все ок
    # json_stream.printSchema()
    # json_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

    # выделим интересующие элементы
    clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"),
                                    col("parsed_value.action").alias("action"))
    # проверим
    # clean_data.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

    users_data = [(1, "Jimmy", 18), (2, "Hank", 48), (3, "Johnny", 9), (4, "Erle", 40)]
    users = spark.createDataFrame(data=users_data, schema=users_schema)

    join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name,
                                                                                         users.user_age,
                                                                                         clean_data.timestamp,
                                                                                         clean_data.action)
    #join_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()
    # блок записи полученных результатов после действий над DataFrame
    console = (join_stream
               .writeStream
               .format('console'))

    console.start().awaitTermination()

    # добавим агрегат - отображать число уникальных id
    #stat_stream = clean_data.groupBy("id").count()
    #stat_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()


