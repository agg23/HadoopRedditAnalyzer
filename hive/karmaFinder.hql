USE reddit;

-- Get the n users with the highest average karma
SELECT author, AVG(score) AS average_karma FROM Comments
    GROUP BY author
    ORDER BY average_karma DESC
    LIMIT ${hiveconf:user_count};

-- Get the n users with the lowest average karma
SELECT author, AVG(score) AS average_karma FROM Comments
    GROUP BY author
    ORDER BY average_karma ASC
    LIMIT ${hiveconf:user_count};
