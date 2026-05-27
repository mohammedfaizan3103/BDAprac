-- Load the data from HDFS
lines = LOAD '/user/pig/wordcount/input/input.txt' AS (line:chararray);

-- Tokenize the lines into individual words (FLATTEN turns the bags into rows)
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Group the records by the word itself
grouped = GROUP words BY word;

-- Count the occurrences in each group
word_counts = FOREACH grouped GENERATE group AS word, COUNT(words) AS count;

-- Store the result back into HDFS
STORE word_counts INTO '/user/pig/wordcount/output';
