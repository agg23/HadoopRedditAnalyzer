USE reddit;

SELECT DATE(CONCAT(year, '-', month, '-', day)) AS loggedDate, COUNT(*) AS traffic
    FROM Comments, Subreddits
    WHERE id = subreddit_id
      AND name = '${hiveconf:subreddit}'
    GROUP BY DATE(CONCAT(year, '-', month, '-', day));
