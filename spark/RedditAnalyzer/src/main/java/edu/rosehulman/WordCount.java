package edu.rosehulman;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordCount {

	class Comment {
		String approved_by;
		String author;
		String banned_by;
		String body;
		String body_html;
		String edited;
		int gilded;
		String parent_id;
		int score;
		String subreddit_id;
	}
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local").appName("WordCount").getOrCreate();
		
		Dataset<Row> rows = spark.read().orc("/Users/adam/Documents/01-9-2006.orc");
		
		Dataset<Row> bodyRows = rows.select("body");
		
//		bodyRows.flatMap(_.getString(0).split(" "), Encoders.STRING());
		
		Dataset<String> split = bodyRows.flatMap(body -> {
			return Arrays.asList(body.getString(0).toLowerCase().split(" ")).iterator();
		}, Encoders.STRING());
		
		Dataset<Row> counts = split.groupBy("value").count().toDF("word", "count");
		
		counts = counts.sort(functions.desc("count"));
		
		counts.show();
		
	
//		bodyRows.flat
	}
}
