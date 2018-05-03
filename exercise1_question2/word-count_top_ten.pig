input_text = LOAD 's3://assignment1/exercise1.txt' AS (line:chararray);
get_words = FOREACH input_text GENERATE FLATTEN(TOKENIZE(line)) as word;
group_words = GROUP get_words BY word;
word_count = FOREACH group_words GENERATE group, COUNT(get_words) as count_words;
Result_order = ORDER word_count BY count_words DESC;
Result_top = LIMIT Result_order 10;
STORE Result_top INTO 's3://assignment1/word_output_top_ten';