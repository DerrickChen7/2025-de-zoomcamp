SELECT * FROM public.green_taxi_trips
LIMIT 100;

SELECT min(lpep_pickup_datetime), max(lpep_pickup_datetime) FROM public.green_taxi_trips;

select * from taxi_zone_lookup;

select count(1) from green_taxi_trips 
where 1=1
and lpep_dropoff_datetime>='2019-10-01 00:00:00'
and lpep_dropoff_datetime<'2019-11-01 00:00:00' 
and trip_distance<=3
and trip_distance>1
;
--104802 198924

select * from green_taxi_trips 
where 1=1
--and lpep_dropoff_datetime>='2019-10-01 00:00:00'
--and lpep_dropoff_datetime<'2019-11-01 00:00:00' 
and trip_distance=515.89
--and trip_distance>1
;

select taxi_zone_lookup."Zone", sum(total_amount) total_amount from green_taxi_trips
join taxi_zone_lookup on green_taxi_trips."PULocationID"=taxi_zone_lookup."LocationID"
where 1=1
and lpep_pickup_datetime>='2019-10-18'
and lpep_pickup_datetime<'2019-10-19'
group by taxi_zone_lookup."Zone"
having sum(total_amount)>=13000
order by sum(total_amount) desc
;

select green_taxi_trips.*, taxi_zone_lookup."Zone",  doloc."Zone" from green_taxi_trips
join taxi_zone_lookup on green_taxi_trips."PULocationID"=taxi_zone_lookup."LocationID"
and taxi_zone_lookup."Zone"='East Harlem North'
join taxi_zone_lookup doloc on green_taxi_trips."DOLocationID"=doloc."LocationID"
where 1=1
and lpep_pickup_datetime>='2019-10-01'
and lpep_pickup_datetime<'2019-11-01'
order by tip_amount desc
;


