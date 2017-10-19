#!/bin/bash
if [ "$#" -gt 4 ] || [ "$#" -lt 2 ]
then
    printf "Usage:\n\tredditDownload.sh [year] [month number] ([HDFS directory]) ([Hive directory])\n"
    exit 1
fi

year="$1"
month="$2"
if [ "$#" -gt 2 ]
then
    hdfsDir="$3"
else
    hdfsDir="/tmp/staging"
fi

filename="RC_$year-$month.bz2"

if [ "$#" -gt 3 ]
then
    hiveDirectory="$4/reddit.db/comments/year=$year/month=$month"
else
    hiveDirectory="/apps/hive/warehouse/reddit.db/comments/year=$year/month=$month"
fi

hadoop fs -mkdir -p "$hdfsDir"

function generateOutput {
    pig -f pig/clean.pig -p inFile="$hdfsDir/$filename" \
        -p outFolder="$hiveDirectory"
    if [ "$?" -ne 0 ]
    then
        echo "Failed to generate output"
        exit 1
    fi

    hive -e 'MSCK REPAIR TABLE reddit.Subreddits'
    hive -e 'MSCK REPAIR TABLE reddit.Comments'

    exit 0
}

hadoop fs -ls "$hdfsDir/$filename" 1> /dev/null 2> /dev/null

if [ "$?" -eq 0 ]
then
    # File exists; don't re-download
    generateOutput
    exit "$?"
fi

mkdir tmp
curl -o "tmp/$filename" "http://files.pushshift.io/reddit/comments/$filename"

if [ $? -ne 0 ]
then
   echo "Failed to download file"
   exit 1
fi

curl -o tmp/sha256sums http://files.pushshift.io/reddit/comments/sha256sums

if [ $? -ne 0 ]
then
   echo "Failed to download checksums"
   exit 1
fi

providedChecksum=$(grep -oP ".+?(?=  $filename)" tmp/sha256sums)

if [ $? -ne 0 ]
then
   echo "Failed to find checksum"
   exit 1
fi

fileChecksum=$(sha256sum tmp/$filename | cut -d " " -f 1)

if [ "$providedChecksum" != "$fileChecksum" ]
then
    echo "Invalid checksum"
   exit 1
else
    echo "Checksum validated. Uploading to HDFS"
fi

hadoop fs -put "tmp/$filename" "$hdfsDir/$filename"

generateOutput
