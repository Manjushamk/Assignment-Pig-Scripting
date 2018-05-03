flight_data = LOAD 's3://assignment1/exercise2.csv' USING PigStorage(',') AS (Year:int, Month:int, DayofMonth:int, DayOfWeek:int, DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, FlightNum:int, TailNum:chararray, ActualElapsedTime:int, CRSElapsedTime:int, AirTime:chararray, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, Distance:int, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int, CancellationCode:chararray, Diverted:int, CarrierDelay:chararray, WeatherDelay:chararray, NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);
flight_without_header = FILTER flight_data BY UniqueCarrier != 'UniqueCarrier';
total_group = GROUP flight_without_header ALL;
total_count = FOREACH total_group GENERATE COUNT(flight_without_header) as count_total;
data_filter = FILTER flight_without_header BY ArrDelay < 15 AND DepDelay < 15;
group_data = GROUP data_filter ALL;
count = FOREACH group_data GENERATE (DOUBLE)COUNT(data_filter)/total_count.count_total AS Proportion, COUNT(data_filter) AS count_of_ontime;
STORE count INTO 's3://assignment1/flight_question1';