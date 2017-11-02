USE reddit;

SELECT AllSubreddits.name, COUNT(*) AS ct
    FROM Comments AS AllComments, Subreddits AS AllSubreddits
    WHERE AllComments.author IN (SELECT SubComments.author
                                     FROM Comments AS SubComments,
                                          Subreddits AS Subreddit
                                     WHERE Subreddit.id = SubComments.subreddit_id
                                       AND Subreddit.name = '${hiveconf:subreddit}')
      AND AllSubreddits.id = AllComments.subreddit_id
      AND AllSubreddits.name <> '${hiveconf:subreddit}'
    GROUP BY AllSubreddits.name
    ORDER BY ct;
