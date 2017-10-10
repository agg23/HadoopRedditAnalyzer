register pig/elephantbird/json-simple-1.1.1.jar;
register pig/elephantbird/elephant-bird-pig-4.15.jar;
register pig/elephantbird/elephant-bird-hadoop-compat-4.15.jar;
register pig/piggybank/piggybank.jar;

DEFINE JsonLoader com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

rawComments = LOAD '$inFile' USING JsonLoader as (json:map[]);

-- Extract proper Pig schema from elephant-bird
comments = FOREACH rawComments GENERATE1
        (chararray)json#'approved_by' as approved_by,
        (chararray)json#'author' as author,
        --(chararray)json#'author_flair_css_class' as author_flair_css_class,
        --(chararray)json#'author_flair_text' as author_flair_text,
        (chararray)json#'banned_by' as banned_by,
        (chararray)json#'body' as body,
        (chararray)json#'body_html' as body_html,
        ToDate(1000 * (long)json#'created_utc') as created_on,        
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

commentAuthors = FOREACH comments GENERATE author;
groupedAuthors = GROUP commentAuthors BY author;
commentorsCounts = FOREACH groupedAuthors
    GENERATE group, COUNT(commentAuthors) AS numComments;

commentsByDay = GROUP comments BY GetDay(created_on);

commentsBySubreddit = GROUP comments BY (subreddit, subreddit_id);

subredditStats = FOREACH commentsBySubreddit {
    gildedComments = FILTER comments BY gilded > 0;

    commentAuthors = FOREACH comments GENERATE author;
    commentors = DISTINCT commentAuthors;

    GENERATE
        SUM(comments.score) as totalScore,
        AVG(comments.score) as averageScore,
        -- I don't understand why using .score works, but without, it fails
        COUNT(gildedComments.score) as totalGilded,
        COUNT(comments.score) as commentCount,
        commentors.author as commentors;
}

STORE commentsByDay INTO '$outFolder'
    USING org.apache.pig.piggybank.storage.MultiStorage('$outFolder', '0', 'bz2', '\\t');