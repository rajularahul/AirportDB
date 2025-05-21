CREATE TABLE dim_airline (
    airline_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    iata CHAR(2),
    airlinename VARCHAR(30),
    base_airport_id SMALLINT,
    base_airport_name VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_airplane (
    airplane_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    capacity INT,
    type_id INT,
    type_identifier VARCHAR(50),
    type_description TEXT,
    airline_id INT
);

CREATE TABLE dim_airport (
    airport_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    iata CHAR(3),
    icao CHAR(4),
    airport_name VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    latitude DECIMAL(11,8),
    longitude DECIMAL(11,8)
);

CREATE TABLE dim_passenger (
    passenger_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    passportno CHAR(9),
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE dim_employee (
    employee_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    birthdate DATE,
    sex CHAR(1),
    department VARCHAR(20),
    salary DECIMAL(8,2),
    city VARCHAR(100),
    country VARCHAR(100)
);
