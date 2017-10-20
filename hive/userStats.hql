USE reddit;

-- Get overall subreddit stats
-- For whatever reason won't let you indent

CREATE TEMPORARY TABLE FilteredComments AS
SELECT * FROM Comments
    WHERE year >= ${hiveconf:minYear}
    AND month >= ${hiveconf:minMonth}
    AND day >= ${hiveconf:minDay}
    AND year <= ${hiveconf:maxYear}
    AND month <= ${hiveconf:maxMonth}
    AND day < ${hiveconf:maxDay};

SELECT COUNT(*), COUNT(DISTINCT author), SUM(score), SUM(gilded), SUM(controversiality) FROM filteredComments;

-- Get individual user stats
SELECT author, COUNT(*), SUM(score), SUM(gilded), SUM(controversiality) FROM filteredComments GROUP BY author;
