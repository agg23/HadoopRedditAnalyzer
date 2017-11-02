# Usage

Setup Hive:
```
hive -f hive/createTables.hql
```

Download a dataset and load it into Hive:
```
./redditDownload.sh 2006 01
```

Generate user stats:
```
hive -f hive/userStats.hql -hiveconf minYear=2006 -hiveconf minMonth=1 -hiveconf minDay=0 -hiveconf maxYear=2007 -hiveconf maxMonth=12 -hiveconf maxDay=32
```

Get the n users with the highest and lowest average and total karma:
```
hive -f hive/karmaFinder.hql -hiveconf user_count=10
```

Get user specific stats, total and divided by subreddit:
```
hive -f hive/lookupUser.hql -hiveconf user="paulgraham" -hiveconf subreddit_count=10
```

Get subreddit specific stats, total and divided by user:
```
hive -f hive/lookupSubreddit.hql -hiveconf subreddit_id="t5_6" -hiveconf user_count=10
```

# Hive Tables

Database: reddit

Tables: Comments, Subreddits

### Comments

Field | Type
--- | ---
approved_by | String
author | String
banned_by | String
body | String
body_html | String
edited | String
gilded | Int
parent_id | String
score | Int
subreddit_id | String

### Subreddits

Field | Type
--- | ---
id | String
name | String

# Oozie Pipelines

## Distances
1. Find most popular `n` subreddits using a Hive query.
2. For each of these most popular subreddits, generate an author table
   using many Hive queries. (These tables have auto-generated names
   and will be deleted at the end of the pipeline.)
3. For each unique pair of these author tables, run a distance query
   to find the size of their union and intersection.
4. Run a quick shell script that takes the difference between the
   union and intersection (to get the XOR distance), adds on the names
   of the subreddits, and appends the result to an output file.
