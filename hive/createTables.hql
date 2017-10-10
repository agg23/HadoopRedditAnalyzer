CREATE TABLE Comments (
      approved_by STRING,
      author STRING,
      banned_by STRING,
      body STRING,
      body_html STRING,
      year INT,
      month INT,
      day INT,
      edited STRING,
      gilded INT,
      parent_id STRING,
      score INT,
      score_hidden BOOLEAN,
      subreddit_id STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ''
    STORED AS TEXTFILE;
