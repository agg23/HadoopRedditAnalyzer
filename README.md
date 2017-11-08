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

Get estimated score for a comment in a subreddit (example /r/programming):
```
spark-submit --class edu.rosehulman.RedditAnalyzer.WordScorePredictor --master yarn --driver-memory 4g --executor-memory 3g --executor-cores 1 RedditAnalyzerSpark-0.0.1-SNAPSHOT.jar "t5_2fwo" "This is the comment I want to test"
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
