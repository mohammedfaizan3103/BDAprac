REGISTER 'is_good_quality.py' USING jython AS myfuncs;
records = LOAD 'weather_data.txt' AS (year:chararray, temp:int, quality:int);
filtered_records = FILTER records BY myfuncs.is_good(quality);
DUMP filtered_records;
