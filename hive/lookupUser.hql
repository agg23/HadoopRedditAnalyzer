USE reddit;

-- Overall user stats
SELECT author, COUNT(*), SUM(score), AVG(score), SUM(gilded)
	FROM Comments
	WHERE author = '${hiveconf:user}'
	GROUP BY author;

-- Subreddit specifc stats
SELECT subreddit_id, COUNT(*) AS comment_count, SUM(score), AVG(score), SUM(gilded)
	FROM Comments
	WHERE author = '${hiveconf:user}'
	GROUP BY subreddit_id
	ORDER BY comment_count
	LIMIT ${hiveconf:subreddit_count};