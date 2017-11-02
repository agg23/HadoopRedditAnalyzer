USE reddit;

CREATE TEMPORARY TABLE MostPopular (
        name STRING,
        commentCount INT
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE MostPopular
    SELECT name, COUNT(*) AS commentCount
        FROM Comments, Subreddits
        WHERE subreddit_id = id
          AND name <> 'reddit.com'
        GROUP BY name
        ORDER BY commentCount DESC
        LIMIT ${hiveconf:count};

CREATE TEMPORARY TABLE Authors (
        subreddit STRING,
        commenter STRING
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE Authors
    SELECT DISTINCT Subreddits.name AS subreddit, author AS commenter
        FROM Comments, Subreddits, MostPopular
        WHERE subreddit_id = id
          AND (Subreddits.name = MostPopular.name
               OR Subreddits.name = '${hiveconf:name}');

DROP TABLE IntersectSizes;

CREATE TABLE IntersectSizes (
        firstSubreddit STRING,
        secondSubreddit STRING,
        intersectSize INT
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE IntersectSizes
    SELECT firstAuthors.subreddit AS firstSubreddit,
           secondAuthors.subreddit AS secondSubreddit,
           COUNT(firstAuthors.commenter) AS intersectSize
        FROM Authors AS firstAuthors,
             Authors AS secondAuthors
        WHERE firstAuthors.subreddit < secondAuthors.subreddit
          AND firstAuthors.commenter = secondAuthors.commenter
        GROUP BY firstAuthors.subreddit, secondAuthors.subreddit;

DROP TABLE ExclusiveSizes;

CREATE TABLE ExclusiveSizes (
        firstSubreddit STRING,
        secondSubreddit STRING,
        exclusiveSize Int
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE ExclusiveSizes
    SELECT firstAuthors.subreddit AS firstSubreddit,
           secondAuthors.subreddit AS secondSubreddit,
           COUNT(firstAuthors.commenter) + COUNT(secondAuthors.commenter)
             - 2*MAX(intersectSize) AS exclusiveSize
        FROM IntersectSizes,
             Authors AS firstAuthors,
             Authors AS secondAuthors
        WHERE firstAuthors.subreddit = IntersectSizes.firstSubreddit
          AND secondAuthors.subreddit = IntersectSizes.secondSubreddit
        GROUP BY firstAuthors.subreddit, secondAuthors.subreddit;
