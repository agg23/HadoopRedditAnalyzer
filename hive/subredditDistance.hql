USE reddit;

-- Returns the number of commenters from both subreddits (by name)

SELECT COUNT(DISTINCT(AllCommenters.commenter))
    FROM (SELECT * FROM ${hiveconf:table_a}
          UNION ALL
          SELECT * FROM ${hiveconf:table_b}) AS AllCommenters;

SELECT COUNT(DISTINCT(ACommenters.commenter))
    FROM ${hiveconf:table_a} AS ACommenters, ${hiveconf:table_b}
    WHERE ACommenters.commenter IN (SELECT * FROM ${hiveconf:table_b});
