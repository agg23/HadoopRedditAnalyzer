USE reddit;

SELECT author, AVG(score) AS average_karma FROM Comments
    GROUP BY author
    ORDER BY average_karma DESC
    LIMIT ${hiveconf:user_count}
