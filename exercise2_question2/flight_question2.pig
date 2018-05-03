flight_data = LOAD 's3://assignment1/exercise2.csv' USING PigStorage(',') AS (Year:int, Month:int, DayofMonth:int, DayOfWeek:int, DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, FlightNum:int, TailNum:chararray, ActualElapsedTime:int, CRSElapsedTime:int, AirTime:chararray, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, Distance:int, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int, CancellationCode:chararray, Diverted:int, CarrierDelay:chararray, WeatherDelay:chararray, NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);
data_filter = FILTER flight_data BY ArrDelay < 15 AND DepDelay < 15;
group_all = GROUP data_filter ALL;
count_total = FOREACH group_all GENERATE COUNT(data_filter) as total_count;
group_data = GROUP data_filter by UniqueCarrier;
result = FOREACH group_data GENERATE group, (DOUBLE)COUNT(data_filter.FlightNum)/count_total.total_count AS count_car;
result_rank = RANK result BY count_car DESC;
STORE result_rank INTO 's3://assignment1/flight_question2';
