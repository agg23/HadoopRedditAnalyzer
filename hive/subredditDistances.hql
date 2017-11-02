USE reddit;

DROP TABLE MostPopular;

CREATE TABLE MostPopular
    AS SELECT name, COUNT(*) AS commentCount
           FROM Comments, Subreddits
           WHERE subreddit_id = id
             AND name <> 'reddit.com'
           GROUP BY name
           ORDER BY commentCount DESC
           LIMIT ${hiveconf:count};

DROP TABLE Authors;

CREATE TABLE Authors
    AS SELECT DISTINCT Subreddits.name AS subreddit, author AS commenter
          FROM Comments, Subreddits, MostPopular
          WHERE subreddit_id = id
            AND (Subreddits.name = MostPopular.name
                 OR Subreddits.name = '${hiveconf:name}');

DROP TABLE UnionSizes;
DROP TABLE IntersectSizes;

CREATE TABLE UnionSizes
    AS SELECT firstAuthors.subreddit AS firstSubreddit,
              secondAuthors.subreddit AS secondSubreddit,
              COUNT(DISTINCT eitherAuthors.commenter) AS unionSize
           FROM Authors AS firstAuthors,
                Authors AS secondAuthors,
                Authors AS eitherAuthors
           WHERE firstAuthors.subreddit < secondAuthors.subreddit
             AND (eitherAuthors.subreddit = firstAuthors.subreddit
                  OR eitherAuthors.subreddit = secondAuthors.subreddit)
           GROUP BY firstAuthors.subreddit, secondAuthors.subreddit;

CREATE TABLE IntersectSizes
    AS SELECT firstAuthors.subreddit AS firstSubreddit,
              secondAuthors.subreddit AS secondSubreddit,
              COUNT(DISTINCT firstAuthors.commenter) AS intersectSize
           FROM Authors AS firstAuthors,
                Authors AS secondAuthors
           WHERE firstAuthors.subreddit < secondAuthors.subreddit
             AND firstAuthors.commenter = secondAuthors.commenter
           GROUP BY firstAuthors.subreddit, secondAuthors.subreddit;

DROP TABLE ExclusiveSizes;

CREATE TABLE ExclusiveSizes
    AS SELECT UnionSizes.firstSubreddit AS firstSubreddit,
              UnionSizes.secondSubreddit AS secondSubreddit,
              unionSize - intersectSize AS distance
           FROM UnionSizes, IntersectSizes
           WHERE UnionSizes.firstSubreddit = IntersectSizes.firstSubreddit
             AND UnionSizes.secondSubreddit = IntersectSizes.secondSubreddit;
