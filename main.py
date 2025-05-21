from pyspark.sql import *
from kafka import KafkaProducer
import json
from pyspark.sql.functions import col, trim
from Extract import extract_data_mysql
from Load import load_data_in_db
from Transform import transform_dim_airline, transform_dim_airport, transform_dim_airplane, transform_dim_passenger, \
    transform_dim_employee, transform_fact_flight_log, transform_fact_booking, transform_fact_flight


def dim_airline_etl_job(spark):
    # dim_airline extraction from airline, airport, airport_geo
    # extraction
    airline_df = extract_data_mysql(spark, "airportdb", "airline")
    airport_df = extract_data_mysql(spark, "airportdb", "airport")
    airport_geo_df = extract_data_mysql(spark, "airportdb", "airport_geo")
    # transformation
    dim_airline = transform_dim_airline(spark, airline_df, airport_df, airport_geo_df)
    # loading
    load_data_in_db("dwh_airport_db", "dim_airline", dim_airline)
    spark.catalog.dropTempView("airline")
    spark.catalog.dropTempView("airport")
    spark.catalog.dropTempView("airport_geo")

def dim_airport_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    airport_df = extract_data_mysql(spark, "airportdb", "airport")
    airport_geo_df = extract_data_mysql(spark, "airportdb", "airport_geo")
    # transformation
    dim_airport = transform_dim_airport(spark, airport_df, airport_geo_df)
    # loading
    load_data_in_db("dwh_airport_db", "dim_airport", dim_airport)
    spark.catalog.dropTempView("airport")
    spark.catalog.dropTempView("airport_geo")

def dim_airplane_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    airplane_df = extract_data_mysql(spark, "airportdb", "airplane")
    airplane_type_df = extract_data_mysql(spark, "airportdb", "airplane_type")
    # transformation
    dim_airplane = transform_dim_airplane(spark,airplane_df,airplane_type_df)
    # loading
    load_data_in_db("dwh_airport_db", "dim_airplane", dim_airplane)
    spark.catalog.dropTempView("airplane")
    spark.catalog.dropTempView("airplane_type")

def dim_passenger_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    passenger_df = extract_data_mysql(spark, "airportdb", "passenger")
    passenger_details_df = extract_data_mysql(spark, "airportdb", "passengerdetails")
    # transformation
    dim_passenger = transform_dim_passenger(spark,passenger_df,passenger_details_df)
    # loading
    load_data_in_db("dwh_airport_db", "dim_passenger", dim_passenger)
    spark.catalog.dropTempView("passenger")
    spark.catalog.dropTempView("passengerdetails")

def dim_employee_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    employee_df = extract_data_mysql(spark, "airportdb", "employee")
    # transformation
    dim_employee = transform_dim_employee(spark,employee_df)
    # loading
    load_data_in_db("dwh_airport_db", "dim_employee", dim_employee)
    spark.catalog.dropTempView("employee")

def fact_flight_log_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    flight_log_df = extract_data_mysql(spark, "airportdb", "flight_log")
    #print(flight_log_df.count())
    # transformation
    fact_flight_log_df = transform_fact_flight_log(spark,flight_log_df)
    # loading
    load_data_in_db("dwh_airport_db", "fact_flight_log", fact_flight_log_df)
    spark.catalog.dropTempView("flight_log")

def fact_flight_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    flight_df = extract_data_mysql(spark, "airportdb", "flight")
    flight_schedule_df = extract_data_mysql(spark, "airportdb", "flightschedule")
    # transformation
    fact_flight = transform_fact_flight(spark,flight_df,flight_schedule_df)
    # loading
    load_data_in_db("dwh_airport_db", "fact_flight", fact_flight)
    spark.catalog.dropTempView("flight")
    spark.catalog.dropTempView("flightschedule")

def fact_booking_etl_job(spark):
    # dim_airport extraction from airport, airport_geo
    # extraction
    booking_df = extract_data_mysql(spark, "airportdb", "booking", partition_column = "booking_id", lower_bound = 1,upper_bound = 10000000,num_partitions = 16)
    booking_df.createOrReplaceTempView("booking")
    for i in range(55000000,55099798,1000000):
    # transformation
        fact_booking_df = transform_fact_booking(spark,booking_df,i,i+1000000)
        # loading
        load_data_in_db("dwh_airport_db", "fact_booking", fact_booking_df)
    spark.catalog.dropTempView("booking")


if __name__=="__main__":
    spark = (SparkSession
             .builder
             .appName("Airport ETL Spark Job")
             .master("local[2]")
             .config("spark.jars", "mysql-connector-j-9.2.0.jar")
             .config("spark.driver.memory", "6g")
             .getOrCreate())

    #dim_airline_etl_job(spark)

    #dim_airport_etl_job(spark)

    #dim_airplane_etl_job(spark)

    #dim_passenger_etl_job(spark)

    #dim_employee_etl_job(spark)

    #fact_flight_log_etl_job(spark)

    #fact_flight_etl_job(spark)

    #fact_booking_etl_job(spark)









