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
score_hidden | Boolean
subreddit_id | String

### Subreddits

Field | Type
--- | ---
id | String
name | String