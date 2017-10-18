register pig/elephantbird/json-simple-1.1.1.jar;
register pig/elephantbird/elephant-bird-pig-4.15.jar;
register pig/elephantbird/elephant-bird-hadoop-compat-4.15.jar;

DEFINE JsonLoader com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

rawComments = LOAD '$inFile' USING JsonLoader as (json:map[]);

-- Extract proper Pig schema from elephant-bird
comments = FOREACH rawComments GENERATE
        (chararray)json#'approved_by' as approved_by,
        (chararray)json#'author' as author,
        --(chararray)json#'author_flair_css_class' as author_flair_css_class,
        --(chararray)json#'author_flair_text' as author_flair_text,
        (chararray)json#'banned_by' as banned_by,
        (chararray)json#'body' as body,
        (chararray)json#'body_html' as body_html,
        (int)GetYear(ToDate(1000 * (long)json#'created_utc')) as year,
        (int)GetMonth(ToDate(1000 * (long)json#'created_utc')) as month,
        (int)GetDay(ToDate(1000 * (long)json#'created_utc')) as day,
        (chararray)json#'edited' as edited,
        (int)json#'gilded' as gilded,
        --(boolean)json#'likes' as likes,
        --(chararray)json#'link_author' as link_author,
        --(chararray)json#'link_id' as link_id,
        --(chararray)json#'link_title' as link_title,
        --(chararray)json#'link_url' as link_url,
        --(int)json#'num_reports' as num_reports,
        (chararray)json#'parent_id' as parent_id,
        --TODO: Handle replies
        --(boolean)json#'saved' as saved,
        (int)json#'score' as score,
        (boolean)json#'score_hidden' as score_hidden,
        (chararray)json#'subreddit' as subreddit,
        (chararray)json#'subreddit_id' as subreddit_id;
        --(chararray)json#'distinguished' as distinguished,

-- Filter out incomplete records
comments = FILTER comments
    BY (score_hidden IS NULL OR score_hidden != true)
        AND (author IS NOT NULL)
        AND (body IS NOT NULL)
        AND (score IS NOT NULL)
        AND (subreddit IS NOT NULL)
        AND (subreddit_id IS NOT NULL);

comments = FOREACH comments GENERATE
    approved_by,
    author,
    banned_by,
    body,
    body_html,
    year,
    month,
    day,
    edited,
    gilded,
    parent_id,
    score,
    subreddit_id;

SPLIT comments INTO
    day1  IF day == 1,
    day2  IF day == 2,
    day3  IF day == 3,
    day4  IF day == 4,
    day5  IF day == 5,
    day6  IF day == 6,
    day7  IF day == 7,
    day8  IF day == 8,
    day9  IF day == 9,
    day10 IF day == 10,
    day11 IF day == 11,
    day12 IF day == 12,
    day13 IF day == 13,
    day14 IF day == 14,
    day15 IF day == 15,
    day16 IF day == 16,
    day17 IF day == 17,
    day18 IF day == 18,
    day19 IF day == 19,
    day20 IF day == 20,
    day21 IF day == 21,
    day22 IF day == 22,
    day23 IF day == 23,
    day24 IF day == 24,
    day25 IF day == 25,
    day26 IF day == 26,
    day27 IF day == 27,
    day28 IF day == 28,
    day29 IF day == 29,
    day30 IF day == 30,
    day31 IF day == 31;

STORE day1  INTO '$outFolder/day=1'  USING OrcStorage();
STORE day2  INTO '$outFolder/day=2'  USING OrcStorage();
STORE day3  INTO '$outFolder/day=3'  USING OrcStorage();
STORE day4  INTO '$outFolder/day=4'  USING OrcStorage();
STORE day5  INTO '$outFolder/day=5'  USING OrcStorage();
STORE day6  INTO '$outFolder/day=6'  USING OrcStorage();
STORE day7  INTO '$outFolder/day=7'  USING OrcStorage();
STORE day8  INTO '$outFolder/day=8'  USING OrcStorage();
STORE day9  INTO '$outFolder/day=9'  USING OrcStorage();
STORE day10 INTO '$outFolder/day=10' USING OrcStorage();
STORE day11 INTO '$outFolder/day=11' USING OrcStorage();
STORE day12 INTO '$outFolder/day=12' USING OrcStorage();
STORE day13 INTO '$outFolder/day=13' USING OrcStorage();
STORE day14 INTO '$outFolder/day=14' USING OrcStorage();
STORE day15 INTO '$outFolder/day=15' USING OrcStorage();
STORE day16 INTO '$outFolder/day=16' USING OrcStorage();
STORE day17 INTO '$outFolder/day=17' USING OrcStorage();
STORE day18 INTO '$outFolder/day=18' USING OrcStorage();
STORE day19 INTO '$outFolder/day=19' USING OrcStorage();
STORE day20 INTO '$outFolder/day=20' USING OrcStorage();
STORE day21 INTO '$outFolder/day=21' USING OrcStorage();
STORE day22 INTO '$outFolder/day=22' USING OrcStorage();
STORE day23 INTO '$outFolder/day=23' USING OrcStorage();
STORE day24 INTO '$outFolder/day=24' USING OrcStorage();
STORE day25 INTO '$outFolder/day=25' USING OrcStorage();
STORE day26 INTO '$outFolder/day=26' USING OrcStorage();
STORE day27 INTO '$outFolder/day=27' USING OrcStorage();
STORE day28 INTO '$outFolder/day=28' USING OrcStorage();
STORE day29 INTO '$outFolder/day=29' USING OrcStorage();
STORE day30 INTO '$outFolder/day=30' USING OrcStorage();
STORE day31 INTO '$outFolder/day=31' USING OrcStorage();