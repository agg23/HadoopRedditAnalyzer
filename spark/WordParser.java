package hadoop.spark.redditanalyzer;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.feature.RegexTokenizer;

import scala.collection.immutable.List;

public class WordParser {
	public static void main(String[] args) {
//		val conf = new SparkConf();
		SparkSession spark = SparkSession
				.builder()
				.appName("Word Parser")
				.enableHiveSupport()
				.getOrCreate();
		spark.sql("CREATE TABLE IF NOT EXISTS redditData (body STRING) USING hive");
		spark.sql("LOAD DATA LOCAL INPATH 'C:\\Users\\pieragab\\Documents\\RedditData\\RC_2005-12\\RC_2005-12' INTO TABLE redditData");
		spark.sql("SELECT * FROM redditData LIMIT 10").show();
		Dataset<Row> commentRow = spark.sql("SELECT body FROM redditData");
//		Row[] rows = (Row[]) commentRow.collect();
//		ArrayList<Comment> comments = new ArrayList<Comment>();
//		for (Row row : rows) {
//			for (int i = 0; i < row.length(); i++) {
//				Comment newC = new Comment((String) row.get(i));
//				comments.add(newC);
//			}	
//		}
		RegexTokenizer regT = new RegexTokenizer()
				.setInputCol("body")
				.setOutputCol("words")
				.setPattern(" ");
		Dataset<Row> tokens = regT.transform(commentRow);
		tokens.select("body", "words").limit(10).show();
//		tokens.groupBy("words");
		spark.sql("CREATE TABLE IF NOT EXISTS wordCounts (word STRING, count INT) USING hive");
		spark.sql("INSERT INTO wordCounts SELECT word, COUNT(word) FROM tokens GROUP BY word");
		spark.sql("SELECT * FROM wordCounts").show();
		
	}

}
