package edu.rosehulman;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordAverageScore {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local").appName("WordAverageScore").getOrCreate();
		
		Dataset<Row> rows = spark.read().orc("/Users/adam/Documents/01-9-2006.orc");
		
		Dataset<Row> bodyRows = rows.select("body", "score");
		
//		bodyRows.flatMap(_.getString(0).split(" "), Encoders.STRING());
		
		
		
		Dataset<String> splitWords = bodyRows.flatMap(row -> {
			return Arrays.asList(row.getString(0).toLowerCase().split(" ")).iterator();
		}, Encoders.STRING());
		
		Dataset<Integer> scores = bodyRows.flatMap(row -> {
			return Arrays.asList(row.getInt(1)).iterator();
		}, Encoders.INT());
		
		bodyRows.map
		
		Dataset<Row> counts = splitWords.groupBy("value").count().toDF("word", "count");
		
		counts = counts.sort(functions.desc("count"));
		
		counts.show();
		
	
//		bodyRows.flat
	}
}
