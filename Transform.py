from pyspark.sql.functions import col, trim

def transform_dim_airline(spark,airline_df,airport_df,airport_geo_df):
    airline_df.createOrReplaceTempView("airline")
    airport_df.createOrReplaceTempView("airport")
    airport_geo_df.createOrReplaceTempView("airport_geo")
    return  spark.sql("""
      SELECT
        a.airline_id,
        a.iata,
        a.airlinename,
        a.base_airport AS base_airport_id,
        ap.name        AS base_airport_name,
        apg.country,
        apg.latitude,
        apg.longitude,
        apg.city
      FROM airline a
      LEFT JOIN airport       ap  ON a.base_airport = ap.airport_id
      LEFT JOIN airport_geo   apg ON ap.airport_id   = apg.airport_id
    """)

def transform_dim_airport(spark,airport_df,airport_geo_df):
    airport_df.createOrReplaceTempView("airport")
    airport_geo_df.createOrReplaceTempView("airport_geo")
    return  spark.sql("""
      SELECT
        ap.airport_id,
        ap.iata,
        ap.icao,
        ap.name        AS airport_name,
        apg.country,
        apg.latitude,
        apg.longitude,
        apg.city
      FROM airport ap
      LEFT JOIN airport_geo   apg ON ap.airport_id   = apg.airport_id
    """)

def transform_dim_airplane(spark,airplane_df,airplane_type_df):
    airplane_df.createOrReplaceTempView("airplane")
    airplane_type_df.createOrReplaceTempView("airplane_type")
    return  spark.sql("""
      SELECT
        aip.capacity,
        aip.type_id,
        aipt.identifier AS type_identifier,
        aipt.description AS type_description,
        aip.airline_id,
        aip.airplane_id
      FROM airplane aip
      LEFT JOIN airplane_type aipt ON aip.type_id = aipt.type_id
    """)

def transform_dim_passenger(spark,passenger_df,passenger_details_df):
    passenger_df.createOrReplaceTempView("passenger")
    passenger_details_df.createOrReplaceTempView("passengerdetails")
    return  spark.sql("""
      SELECT
        p.passenger_id,
        p.passportno,
        p.firstname,
        p.lastname,
        pd.street AS address,
        pd.city,
        pd.sex AS gender,
        pd.birthdate,
        pd.zip
      FROM passenger p
      LEFT JOIN passengerdetails pd ON p.passenger_id = pd.passenger_id
    """)

def transform_dim_employee(spark,employee_df):
    employee_df.createOrReplaceTempView("employee")
    return  spark.sql("""
      SELECT
          firstname, lastname, birthdate, sex, department, salary, city, country, employee_id
      FROM employee
    """)

def transform_fact_flight_log(spark,flight_log_df):
    flight_log_df.createOrReplaceTempView("flight_log")
    return  spark.sql("""
      SELECT
          flight_log_id, log_date, user, flight_id, flightno_old, flightno_new, from_old, to_old, from_new, to_new, departure_old, arrival_old, departure_new, arrival_new, airplane_id_old, airplane_id_new, airline_id_old, airline_id_new, comment
      FROM flight_log
    """)

def transform_fact_flight(spark,flight_df,flight_schedule_df):
    flight_df.createOrReplaceTempView("flight")
    flight_schedule_df.createOrReplaceTempView("flightschedule")
    return  spark.sql("""
      select
        f.flight_id , f.flightno , f.from AS from_airport_id, f.to AS to_airport_id, f.departure, f.arrival, f.airline_id, f.airplane_id, fs.departure AS schedule_departure, fs.arrival AS schedule_arrival, fs.monday, fs.tuesday, fs.wednesday, fs.thursday, fs.friday, fs.saturday, fs.sunday
        from flight f
        LEFT JOIN flightschedule fs ON f.flightno = fs.flightno;
    """)


def transform_fact_booking(spark, booking_df, booking_id1, booking_id2):

    # Using f-string for dynamic insertion
    query = f"""
      SELECT
          booking_id, flight_id, passenger_id, seat, price
      FROM booking 
      WHERE booking_id >= {booking_id1} AND booking_id < {booking_id2} order by booking_id asc
    """
    return spark.sql(query)
