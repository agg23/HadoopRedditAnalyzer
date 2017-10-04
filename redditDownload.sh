#!/bin/bash
if [ "$#" -ne 3 ]
then
    printf "Usage:\n\tredditDownload.sh [year] [month number] [HDFS directory]\n"
    exit 1
fi

filename="RC_$1-$2.bz2"

mkdir tmp
curl -o tmp/RC_$1-$2.bz2 http://files.pushshift.io/reddit/comments/$filename

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

hadoop fs -put "tmp/$filename" "$3/$filename"