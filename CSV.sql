select t.ticket_no , t.passenger_id , t.passenger_name , tf.flight_id , tf.fare_conditions , tf.amount,
f.flight_no, f.scheduled_departure, f.actual_departure , f.departure_airport , a3.airport_name as airpotr_departure, a3.city as city_departure ,f.scheduled_arrival, f.actual_arrival, f.arrival_airport ,
  a2.airport_name as airport_arrival , a2.city as city_arrival, f.status , s.aircraft_code, a.model, s.seats_cnt
from bookings.tickets t 
right join bookings.ticket_flights tf 
on t.ticket_no =tf.ticket_no 
full join bookings.flights f 
on tf.flight_id =f.flight_id 
left  join bookings.aircrafts a 
on f.aircraft_code = a.aircraft_code 
left join (
	select s.aircraft_code, count(s.aircraft_code) as seats_cnt 
	from bookings.seats s 
	group by s.aircraft_code ) s 
on a.aircraft_code =s.aircraft_code
left join bookings.airports a2 
on f.arrival_airport =a2.airport_code 
left join bookings.airports a3 
on f.departure_airport  =a3.airport_code 
where scheduled_departure >  '{d1}' and  
scheduled_departure < '{d2}'