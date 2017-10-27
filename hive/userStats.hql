USE reddit;

-- Get overall subreddit stats
-- For whatever reason won't let you indent

CREATE VIEW FilteredComments AS
SELECT * FROM Comments
    WHERE DATE(CONCAT(year, '-', month, '-', day))
            >= DATE(CONCAT(${hiveconf:minYear}, '-',
                           ${hiveconf:minMonth}, '-',
                           ${hiveconf:minDay}))
      AND DATE(CONCAT(year, '-', month, '-', day))
            <= DATE(CONCAT(${hiveconf:maxYear}, '-',
                           ${hiveconf:maxMonth}, '-',
                           ${hiveconf:maxDay}));

SELECT COUNT(*),
       COUNT(DISTINCT author),
       SUM(score),
       SUM(gilded),
       SUM(controversiality)
    FROM FilteredComments;

-- Get individual user stats
SELECT author,
       COUNT(*),
       SUM(score),
       SUM(gilded),
       SUM(controversiality)
    FROM FilteredComments
    GROUP BY author;

DROP VIEW FilteredComments;
