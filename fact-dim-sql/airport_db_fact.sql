CREATE TABLE fact_flight (
    flight_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    flightno CHAR(8),
    from_airport_id SMALLINT,
    to_airport_id SMALLINT,
    departure DATETIME,
    arrival DATETIME,
    airline_id SMALLINT,
    airplane_id INT,
    schedule_departure TIME,
    schedule_arrival TIME,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN
);

CREATE TABLE fact_booking (
    booking_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    flight_id INT,
    passenger_id INT,
    seat CHAR(4),
    price DECIMAL(10,2)
);

CREATE TABLE fact_flight_log (
    flight_log_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    flight_id INT,
    log_date DATETIME,
    user VARCHAR(100),
    flightno_old CHAR(8),
    flightno_new CHAR(8),
    from_old SMALLINT,
    to_old SMALLINT,
    from_new SMALLINT,
    to_new SMALLINT,
    departure_old DATETIME,
    arrival_old DATETIME,
    departure_new DATETIME,
    arrival_new DATETIME,
    airplane_id_old INT,
    airplane_id_new INT,
    airline_id_old SMALLINT,
    airline_id_new SMALLINT,
    comment VARCHAR(200)
);
