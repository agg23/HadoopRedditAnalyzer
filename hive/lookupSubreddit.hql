USE reddit;

-- Subreddit stats
SELECT subreddit_id, COUNT(*), SUM(score), AVG(score), SUM(gilded), AVG(gilded), COUNT(DISTINCT author)
	FROM Comments
	WHERE subreddit_id = '${hiveconf:subreddit_id}'
	GROUP BY subreddit_id;

-- Top contributors
SELECT subreddit_id, author, COUNT(*) AS comment_count, SUM(score), AVG(score), SUM(gilded)
	FROM Comments
	WHERE subreddit_id = '${hiveconf:subreddit_id}'
	GROUP BY subreddit_id, author
	ORDER BY comment_count DESC
	LIMIT ${hiveconf:user_count};

-- Highest karma contributors
SELECT subreddit_id, author, COUNT(*), SUM(score) as total_karma, AVG(score), SUM(gilded)
	FROM Comments
	WHERE subreddit_id = '${hiveconf:subreddit_id}'
	GROUP BY subreddit_id, author
	ORDER BY total_karma DESC
	LIMIT ${hiveconf:user_count};

-- Lowest karma contributors
SELECT subreddit_id, author, COUNT(*), SUM(score) as total_karma, AVG(score), SUM(gilded)
	FROM Comments
	WHERE subreddit_id = '${hiveconf:subreddit_id}'
	GROUP BY subreddit_id, author
	ORDER BY total_karma ASC
	LIMIT ${hiveconf:user_count};
