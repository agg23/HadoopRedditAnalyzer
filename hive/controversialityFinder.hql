USE reddit;

-- Get the n users with the highest average controversiality
SELECT author, AVG(controversiality) AS average_controversiality
    FROM Comments, Subreddits
    WHERE id = subreddit_id
      AND name = '${hiveconf:subreddit}'
    GROUP BY author
    ORDER BY average_controversiality DESC
    LIMIT ${hiveconf:user_count};

-- Get the n users with the lowest average controversiality
SELECT author, AVG(controversiality) AS average_controversiality
    FROM Comments, Subreddits
    WHERE id = subreddit_id
      AND name = '${hiveconf:subreddit}'
    GROUP BY author
    ORDER BY average_controversiality ASC
    LIMIT ${hiveconf:user_count};

-- Get the n users with the highest total controversiality
SELECT author, SUM(controversiality) AS total_controversiality
    FROM Comments, Subreddits
    WHERE id = subreddit_id
      AND name = '${hiveconf:subreddit}'
    GROUP BY author
    ORDER BY total_controversiality DESC
    LIMIT ${hiveconf:user_count};

-- Get the n users with the lowest total controversiality
SELECT author, SUM(controversiality) AS total_controversiality
    FROM Comments, Subreddits
    WHERE id = subreddit_id
      AND name = '${hiveconf:subreddit}'
    GROUP BY author
    ORDER BY total_controversiality ASC
    LIMIT ${hiveconf:user_count};
