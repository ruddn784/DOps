create schema nda

drop table nda.flights

select *
from nda.flights f where f.scheduled_departure > '2015-12-30';

drop table  nda.flights 

select count(*) from nda.flights f 

select distinct scheduled_departure  from nda.flights f

select * from nda.flights f 

create  table nda.flights (
flight_id integer ,
flight_no varchar (20),
--ticket_no varchar,
scheduled_departure timestamptz,
actual_departure  timestamptz,
departure_airport varchar (20),
scheduled_arrival timestamptz,
actual_arrival timestamptz,
arrival_airport varchar (20),
status varchar (20) ,
aircraft_code varchar (20),
PRIMARY KEY (flight_id),
FOREIGN KEY (aircraft_code) REFERENCES nda.aircraft(aircraft_code),
FOREIGN KEY (arrival_airport) REFERENCES nda.airport(airport_code),
FOREIGN KEY (departure_airport)REFERENCES nda.airport(airport_code)
--TABLE "ticket_flights" FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
)

select * from nda.aircraft a 

drop table nda.aircraft 

create  table nda.aircraft(
aircraft_code char (3),
model varchar (20),
seats_cnt integer,
PRIMARY KEY (aircraft_code)
)

select * from nda.airport a 
--where airport_code = 'AAQ'

delete  from  nda.airport 
where airport_code = 'AAQ'

drop table nda.airport

select * from nda.airport a 

select count(*) from nda.airport a 

create  table nda.airport(
airport_code char (3),
airport_name varchar (20),
city varchar (25),
PRIMARY KEY (airport_code))


select * from nda.ticket_flight tf 

select count(*) from nda.ticket_flight tf  

create  table nda.ticket_flight(
ticket_no varchar (20),
flight_id integer,
passenger_id varchar (20),
fare_conditions varchar (20),
amount numeric(10,2),
PRIMARY KEY (ticket_no, flight_id),
FOREIGN KEY (flight_id) REFERENCES nda.flights(flight_id),
--FOREIGN KEY (ticket_no) REFERENCES nda.tickets(ticket_no),
FOREIGN KEY (passenger_id) REFERENCES nda.passenger(passenger_id)
)

drop table nda.ticket_flight 

drop table nda.passenger

--create  table stage.passenger(

select * from nda.passenger p

select distinct passenger_id  from nda.passenger p

select p.pas
from(
select count(passenger_id) as pas from nda.passenger  
group by passenger_id) as p
where p.pas= 1


create  table nda.passenger(
passenger_id varchar (20),-- not null,
passenger_name varchar (25),
PRIMARY KEY (passenger_id)
)



