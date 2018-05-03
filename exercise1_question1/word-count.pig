input_text = LOAD 's3://assignment1/exercise1.txt' AS (line:chararray);
get_words = FOREACH input_text GENERATE FLATTEN(TOKENIZE(line)) as word;
group_words = GROUP get_words BY word;
word_count = FOREACH group_words GENERATE group, COUNT(get_words);
STORE word_count INTO 's3://assignment1/word_output';
