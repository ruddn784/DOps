create schema dds


select * from dds.dim_calendar dc  

create table dds.Dim_Calendar -- справочник дат
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('2010-01-01'::timestamp
            , '2030-01-01'::timestamp
            , '1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::int AS id,
    dt AS date,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day
    FROM dates
ORDER BY dt;

ALTER TABLE dds.Dim_Calendar ADD PRIMARY KEY (id);

--drop table dds.Dim_Calendar

--select * from dds.Dim_Calendar


drop table dds.Dim_Passengers 

drop table dds.fact_flights 

select * from dds.dim_passengers dp 

select distinct passenger_id  from dds.dim_passengers

create table dds.Dim_Passengers (-- справочник пассажиров. 
id serial  primary key,
passenger_id varchar(20) not null UNIQUE, 
passenger_name varchar(25) not null          
--ticket_no bpchar
)

select * from  dds.dim_aircrafts 

create table dds.Dim_Aircrafts -- справочник самолетов
(aircraft_id serial primary key,
aircraft_code varchar(4) not null UNIQUE,
model varchar(50) not null, 
seats_cnt integer
)
--aircraft_range int not null)

drop table dds.Dim_Aircrafts 

select * from dds.dim_airports da 

truncate table dds.dim_aircrafts 

create table dds.Dim_Airports -- справочник аэропортов
(airport_id serial primary key,
airport_code varchar (5)  not null UNIQUE,
airport_name varchar(50) not null,
city varchar(100) not null 
--timezone varchar(100) not null
)

--truncate dds.Dim_Airports


drop table dds.Dim_Airports


create table dds.Dim_Tariff -- справочник тарифов
(id serial not null primary key,
--flight_id char(6) not null primary key,
fare_conditions varchar(10)  not null  UNIQUE
--amount float8 not null
--status varchar(15) not null,
--effective_ts date not null,
--expire_ts date not null
)

select * from dds.dim_tariff dt 

drop table dds.Dim_Tariff



create table dds.Fact_Flights (
id serial not null primary key,
flight_id integer not null,
flight_no char (6),
pass_id int not null references dds.Dim_Passengers (id),
date_departure_id int not null references dds.Dim_Calendar (id),
date_arrival_id int not null references dds.Dim_Calendar (id),
--actual_departure timestamp,
--actual_arrival timestamp,
delay_departure int, --timestamp,
delay_arrival int, --timestamp,
departure_airport_id int not null references dds.Dim_Airports (airport_id),
arrival_airport_id int not null references dds.Dim_Airports (airport_id),
aircraft_id int not null references dds.Dim_Aircrafts (aircraft_id),
tariff int not null references dds.Dim_Tariff(id),
price float8,
ticket_no varchar
)

select * from dds.fact_flights ff 

select distinct  date_departure_id  from dds.fact_flights ff 

select count(*) from dds.fact_flights ff 

 

drop table dds.Fact_Flights


