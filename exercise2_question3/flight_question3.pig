flight_data = LOAD 's3://assignment1/exercise2.csv' USING PigStorage(',') AS (Year:int, Month:int, DayofMonth:int, DayOfWeek:int, DepTime:int, CRSDepTime:int, ArrTime:int, CRSArrTime:int, UniqueCarrier:chararray, FlightNum:int, TailNum:chararray, ActualElapsedTime:int, CRSElapsedTime:int, AirTime:chararray, ArrDelay:int, DepDelay:int, Origin:chararray, Dest:chararray, Distance:int, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int, CancellationCode:chararray, Diverted:int, CarrierDelay:chararray, WeatherDelay:chararray, NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);
data_filter = FILTER flight_data BY UniqueCarrier != 'UniqueCarrier';
frequency_table = FOREACH data_filter GENERATE Origin,Dest;
result_group = GROUP frequency_table BY (Origin,Dest);
result_count = FOREACH result_group GENERATE group, COUNT(frequency_table) as count;
result_order = ORDER result_count BY count DESC;
STORE result_order INTO 's3://assignment1/flight_question3';