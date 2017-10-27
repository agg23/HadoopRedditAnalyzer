USE reddit;

SELECT * FROM Comments, Subreddits
    WHERE DATE(CONCAT(year, '-', month, '-', day))
            >= DATE(CONCAT(${hiveconf:minYear}, '-',
                           ${hiveconf:minMonth}, '-',
                           ${hiveconf:minDay}))
      AND DATE(CONCAT(year, '-', month, '-', day))
            <= DATE(CONCAT(${hiveconf:maxYear}, '-',
                           ${hiveconf:maxMonth}, '-',
                           ${hiveconf:maxDay}))
      AND subreddit_id = id
      AND name = '${hiveconf:subreddit}';
