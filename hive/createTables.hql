CREATE TABLE Comments (
      approved_by STRING,
      author STRING,
      banned_by STRING,
      body STRING,
      body_html STRING,
      edited STRING,
      gilded INT,
      parent_id STRING,
      score INT,
      score_hidden BOOLEAN,
      subreddit_id STRING
    )   
    PARTITIONED BY (year INT, month INT, day INT)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;
