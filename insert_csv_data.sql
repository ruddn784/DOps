CREATE OR REPLACE FUNCTION public.insert_csv_data(csv_string text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    csv_rows text[];
    csv_row text;
BEGIN
    -- Разделить CSV-строку на отдельные строки
    csv_rows := string_to_array(csv_string, E'\n');
    csv_rows = (select array_remove(csv_rows, 'ticket_no,passenger_id,passenger_name,flight_id,fare_conditions,amount,flight_no,scheduled_departure,actual_departure,departure_airport,airpotr_departure,city_departure,scheduled_arrival,actual_arrival,arrival_airport,airport_arrival,city_arrival,status,aircraft_code,model,seats_cnt\n'));
  	--csv_rows = (select array_remove(csv_rows, [1]))
   -- Проход по каждой строке CSV-строки
    FOREACH csv_row IN ARRAY csv_rows
    LOOP
        -- Вставить данные в таблицу
        INSERT INTO stage (ticket_no, passenger_id,  passenger_name, flight_id , fare_conditions, amount, flight_no,
scheduled_departure, actual_departure, departure_airport, airpotr_departure, city_departure, scheduled_arrival,
actual_arrival, arrival_airport, airport_arrival, city_arrival, status, aircraft_code, model, seats_cnt)
        VALUES (split_part(csv_row, ',', 1), split_part(csv_row, ',', 2), split_part(csv_row, ',', 3), split_part(csv_row, ',', 4)
       , split_part(csv_row, ',', 5), split_part(csv_row, ',', 6), split_part(csv_row, ',', 7), split_part(csv_row, ',', 8)
      , split_part(csv_row, ',', 9), split_part(csv_row, ',', 10), split_part(csv_row, ',', 11), split_part(csv_row, ',', 12)
     , split_part(csv_row, ',', 13), split_part(csv_row, ',', 14), split_part(csv_row, ',', 15), split_part(csv_row, ',', 16)
    , split_part(csv_row, ',', 17), split_part(csv_row, ',', 18), split_part(csv_row, ',', 19), split_part(csv_row, ',', 20)
   , split_part(csv_row, ',', 21));
    END LOOP;
    
    -- Вернуть значение void
    RETURN;
END;