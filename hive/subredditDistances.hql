USE reddit;

----------------------------------------
-- Find the top <count> subreddits    --
-- by the number of comments on them. --
----------------------------------------
CREATE TEMPORARY TABLE MostPopular (
        name STRING,
        commentCount INT
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE MostPopular
    SELECT name,
           COUNT(*) AS commentCount
        FROM Comments, Subreddits
        WHERE subreddit_id = id
          AND name <> 'reddit.com'
          AND DATE(CONCAT(year, '-', month, '-', day)) >= DATE('${hiveconf:minDate}')
          AND DATE(CONCAT(year, '-', month, '-', day)) <= DATE('${hiveconf:maxDate}')
        GROUP BY name
        ORDER BY commentCount DESC
        LIMIT ${hiveconf:count};


-------------------------------------
-- Stores all commenters with each --
-- subreddit they comment on.      --
-------------------------------------
CREATE TEMPORARY TABLE Authors (
        subreddit STRING,
        commenter STRING
    )
    STORED AS ORC;

INSERT OVERWRITE TABLE Authors
    SELECT DISTINCT Subreddits.name AS subreddit, author AS commenter
        FROM Comments, Subreddits, MostPopular
        WHERE subreddit_id = id
          AND DATE(CONCAT(year, '-', month, '-', day)) >= DATE('${hiveconf:minDate}')
          AND DATE(CONCAT(year, '-', month, '-', day)) <= DATE('${hiveconf:maxDate}')
          AND (Subreddits.name = MostPopular.name
               OR Subreddits.name = '${hiveconf:name}');


----------------------------------------
-- Gets the sizes of the intersection --
-- between each pair of subreddits.   --
----------------------------------------
DROP TABLE IntersectSizes;

CREATE TABLE IntersectSizes (
        firstSubreddit STRING,
        secondSubreddit STRING,
        intersectSize INT
    )
    STORED AS ORC;

-- Note the "less than" which allows us to get
--   non-symmetric, non-reflexive pairs.
INSERT OVERWRITE TABLE IntersectSizes
    SELECT firstAuthors.subreddit AS firstSubreddit,
           secondAuthors.subreddit AS secondSubreddit,
           COUNT(firstAuthors.commenter) AS intersectSize
        FROM Authors AS firstAuthors,
             Authors AS secondAuthors
        WHERE firstAuthors.subreddit < secondAuthors.subreddit
          AND firstAuthors.commenter = secondAuthors.commenter
        GROUP BY firstAuthors.subreddit, secondAuthors.subreddit;

------------------------------------------
-- Gets the "distance" between two      --
-- subreddits as the size of their XOR. --
------------------------------------------
DROP TABLE ExclusiveSizes;

CREATE TABLE ExclusiveSizes (
        firstSubreddit STRING,
        secondSubreddit STRING,
        exclusiveSize Int
    )
    STORED AS ORC;

-- We need the MAX to aggregate together the intersectSize,
--   even though there's only one value.
-- The normal way to find the XOR would use the Union instead of
--   the individual sizes, but after some complexity analysis, that
--   is cubic while this is squared complexity.
-- We match up the first and second authors to the intersect's
--   subreddits solely so we can get their counts (is there a better way?).
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
