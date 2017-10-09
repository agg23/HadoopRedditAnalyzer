register elephantbird/json-simple-1.1.1.jar;
register elephantbird/elephant-bird-pig-4.15.jar;
register elephantbird/elephant-bird-hadoop-compat-4.15.jar;

DEFINE JsonLoader com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

rawComments = LOAD 'RC_2005-12' USING JsonLoader as (json:map[]);

-- Extract proper Pig schema from elephant-bird
comments = FOREACH rawComments GENERATE
        (chararray)json#'approved_by' as approved_by,
        (chararray)json#'author' as author,
        --(chararray)json#'author_flair_css_class' as author_flair_css_class,
        --(chararray)json#'author_flair_text' as author_flair_text,
        (chararray)json#'banned_by' as banned_by,
        (chararray)json#'body' as body,
        (chararray)json#'body_html' as body_html,
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

commentsBySubreddit = GROUP comments by (subreddit_id, subreddit);

subredditStats = FOREACH commentsBySubreddit {
    gildedComments = FILTER comments BY gilded > 0;
    commentAuthors = FOREACH comments GENERATE author;
    commentors = DISTINCT commentAuthors;
    GENERATE
        SUM(comments.score) as totalScore,
        AVG(comments.score) as averageScore,
        COUNT(gildedComments) as totalGilded,
        -- I don't understand why using .score works, but without, it fails
        COUNT(comments.score) as commentCount,
        commentors as commentors;
}