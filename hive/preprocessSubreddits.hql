USE reddit;

CREATE TEMPORARY TABLE IF NOT EXISTS SubredditsTemp (
      id STRING,
      name STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS ORC;

LOAD DATA INPATH '${hiveconf:subredditsInput}' OVERWRITE INTO TABLE SubredditsTemp;

INSERT INTO Subreddits SELECT * FROM SubredditsTemp;

-- TODO: Handle merging new subreddits in