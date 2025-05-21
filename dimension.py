from pyspark.sql import *
from kafka import KafkaProducer
import json
from pyspark.sql.functions import col, trim

"""producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)"""

"""data = [row.asDict() for row in airline_df.collect()]
producer.send("airline-dimension", value=data)
producer.close()"""

def extract_data_mysql(spark,table_name):
    extracted_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/airportdb") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "R@hul123") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    print(f"Data Extracted For Table Name : {table_name}")
    extracted_df.show(5)
    return extracted_df

def extract_airline_data(spark):
    airline_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/airportdb") \
        .option("dbtable", "airline") \
        .option("user", "root") \
        .option("password", "R@hul123") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    airline_df.show(5)
    airline_df=(airline_df.withColumnRenamed("iata","iata_code")
                .withColumnRenamed("airlinename","airline_name"))
    return airline_df

def extract_airport_data(spark):
    airport_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/airportdb") \
        .option("dbtable", "airport") \
        .option("user", "root") \
        .option("password", "R@hul123") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    airport_df = airport_df.withColumnRenamed("name", "airport_name")
    airport_df.show(5)
    return  airport_df

def extract_airport_geo_data(spark):
    airport_geo_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/airportdb") \
        .option("dbtable", "airport_geo") \
        .option("user", "root") \
        .option("password", "R@hul123") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    airport_geo_df.show(5)
    return  airport_geo_df

def transform_dim_airport(airport_df,airport_geo_df):
    dim_airport_df=airport_df.join(airport_geo_df,on="airport_id",how="left")
    dim_airport_df=dim_airport_df.select(
        col("airport_id"),
        trim(col("airport_name")).alias("airport_name"),
        trim(col("city")).alias("city"),
        trim(col("country")).alias("country"),
        trim(col("IATA")).alias("IATA"),
        trim(col("ICAO")).alias("ICAO"),
        col("latitude").cast("decimal(9,6)"),
        col("longitude").cast("decimal(9,6)")
    )
    return  dim_airport_df

def load_data_in_db(dim_df,table_name):
    dim_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/airportdb") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "R@hul123") \
        .mode("append") \
        .save()

if __name__=="__main__":
    spark = (SparkSession
             .builder
             .appName("hello spark")
             .master("local[2]")
             .config("spark.jars", "mysql-connector-j-9.2.0.jar")
             .getOrCreate())
    airline_df=extract_airline_data(spark)
    load_data_in_db(airline_df,"dim_airline")
