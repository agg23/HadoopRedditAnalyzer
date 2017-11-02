USE reddit;

CREATE TABLE ${hiveconf:table}
    AS SELECT DISTINCT(author) AS commenter
          FROM Comments, Subreddits
          WHERE subreddit_id = id
            AND name = '${hiveconf:subreddit}';