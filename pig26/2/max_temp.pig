-- Load the raw weather records
raw_data = LOAD '/user/pig/ncdc/input/ncdc_data.txt' AS (line:chararray);

-- Extract Year (columns 15-19) and Temperature (columns 87-92)
-- Temperature is divided by 10 because NCDC stores 25.0 as 0250
filtered_data = FOREACH raw_data GENERATE 
    SUBSTRING(line, 15, 19) AS year, 
    (int)SUBSTRING(line, 87, 92) AS temp;

-- Filter out "missing" data (NCDC uses +9999 for missing values)
clean_data = FILTER filtered_data BY temp != 9999;

-- Group by Year
grouped_year = GROUP clean_data BY year;

-- Find the Maximum Temperature
max_temp = FOREACH grouped_year GENERATE group AS year, MAX(clean_data.temp) AS max_val;

-- Store the results
STORE max_temp INTO '/user/pig/ncdc/output';
