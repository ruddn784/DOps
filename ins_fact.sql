INSERT INTO dds.Fact_Flights (
            flight_id, flight_no, pass_id, date_departure_id, date_arrival_id, delay_departure, delay_arrival,
            departure_airport_id, arrival_airport_id,
            aircraft_id, tariff, price, ticket_no
        )
SELECT
    f.flight_id,
    f.flight_no,
    dp.id AS pass_id,
    dc.id AS date_departure_id,
    dc2.id AS date_arrival_id,
    f.delay_departure,
    f.delay_arrival,
    da.airport_id AS departure_airport_id, 
    da2.airport_id AS arrival_airport_id,
    da3.aircraft_id AS aircraft_id, 
    dt.id AS tariff,
    f.amount AS price,
    f.ticket_no    
FROM 
    (SELECT
        f2.flight_id, 
        f2.flight_no, 
        f2.actual_departure::date, 
        f2.departure_airport,
        f2.actual_arrival::date, 
        f2.arrival_airport, 
        f2.status, 
        f2.aircraft_code,
        EXTRACT(HOUR FROM AGE(actual_departure,scheduled_departure )) as delay_departure,
        EXTRACT(HOUR FROM AGE(actual_arrival,scheduled_arrival )) as delay_arrival,
        tf.passenger_id, 
        tf.fare_conditions, 
        tf.amount, 
        tf.ticket_no
    FROM nda.flights f2 
    LEFT JOIN nda.ticket_flight tf 
        ON f2.flight_id = tf.flight_id 
     ) AS f
LEFT JOIN dds.dim_passengers dp  
    ON f.passenger_id = dp.passenger_id 
LEFT JOIN dds.dim_calendar dc 
    ON f.actual_departure::date = dc."date"
LEFT JOIN dds.dim_calendar dc2 
    ON f.actual_arrival::date = dc2."date"
LEFT JOIN dds.dim_airports da  
    ON f.departure_airport = da.airport_code
LEFT JOIN dds.dim_airports da2 
    ON f.arrival_airport = da2.airport_code
LEFT JOIN dds.dim_aircrafts da3 
    ON f.aircraft_code = da3.aircraft_code
LEFT JOIN dds.dim_tariff dt 
    ON f.fare_conditions = dt.fare_conditions
where dp.id notnull