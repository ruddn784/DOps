--create schema stage

drop table stage

truncate  table stage 

select *
from stage s ;

select count(*) from stage s 

create  table stage (
ticket_no varchar (20),
passenger_id varchar (20),
passenger_name varchar (25),
flight_id varchar (20),
fare_conditions varchar (20),
amount varchar (20),
flight_no varchar (20),
scheduled_departure varchar (25),
actual_departure varchar (25)á
departure_airport varchar (20),
airpotr_departure varchar (20),
city_departure varchar (25),
scheduled_arrival varchar (25),
actual_arrival varchar (25),
arrival_airport varchar (20),
airport_arrival varchar (20),
city_arrival varchar (25),
status varchar (20),
aircraft_code varchar (20),
model varchar (20),
seats_cnt integer 
)

select departure_airport  as airport_code, airpotr_departure  as airport_name,city_departure as city
from stage 
union
select arrival_airport  as airport_code, airport_arrival  as airport_name, city_arrival city
from stage



drop table flights

select *
from flights f  ;

select count(*) from flights f 

truncate  table flights

create  table flights (
flight_id varchar (20) not null,
flight_no varchar (20),
--ticket_no varchar,
scheduled_departure varchar (25),
actual_departure  varchar (25),
departure_airport varchar (20) not null,
scheduled_arrival varchar (25) ,
actual_arrival varchar (25) ,
arrival_airport varchar (20) not null,
status varchar (20) ,
aircraft_code varchar (20) not null
)

drop table aircraft 

truncate table aircraft 

select * from aircraft a 

create  table aircraft(
aircraft_code varchar (20) not null,
model varchar (20),
seats_cnt varchar (20)
)

drop table airport 

truncate  table airport 

select * from airport 

--SELECT airport_code, airport_name, city FROM stage

create  table airport(
airport_code varchar (20) not null,
airport_name varchar (20),
city varchar (25)
)

drop table ticket_flight

truncate table ticket_flight 

select * from ticket_flight 

create  table ticket_flight(
ticket_no varchar (20) not null,
flight_id varchar (20) not null,
passenger_id varchar (20) not null,
fare_conditions varchar (20) not null,
amount varchar (20)
)

drop table passenger

--create  table stage.passenger(
create  table passenger(
passenger_id varchar (20),-- not null,
passenger_name varchar (25),
CONSTRAINT notnul check (passenger_id is not null),
CONSTRAINT unic  unique (passenger_id))



select * from passenger p  ;

truncate  table passenger

insert into stage.passenger (passenger_id ,passenger_name )
select s.passenger_id , s.passenger_name 
from  stage.stage as s  
--on conflict ON CONSTRAINT notnul DO nothing  
on conflict (passenger_id) where passenger_id =null  DO nothing
