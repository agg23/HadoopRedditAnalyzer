USE reddit;

-- Returns the number of commenters from both subreddits (by name)

CREATE TEMPORARY TABLE ACommenters
    AS SELECT DISTINCT(author) AS commenter
          FROM Comments, Subreddits
          WHERE subreddit_id = id
            AND name = '${hiveconf:subreddit_a}';

CREATE TEMPORARY TABLE BCommenters
    AS SELECT DISTINCT(author) AS commenter
          FROM Comments, Subreddits
          WHERE subreddit_id = id
            AND name = '${hiveconf:subreddit_b}';

SELECT COUNT(DISTINCT(aCommenters.commenter))
    FROM ACommenters AS aCommenters, BCommenters AS bCommenters
    WHERE aCommenters.commenter IN (SELECT * FROM bCommenters);

DROP TABLE ACommenters;
DROP TABLE BCommenters;
