package hadoop.spark.redditanalyzer;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("WordCount")
				.getOrCreate();
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> words = lines.flatMap(a -> Arrays.asList(Pattern.compile(" ").split(a)).iterator());
		
		JavaPairRDD<String, Integer> wordMap = words.mapToPair(s -> new Tuple2<>(s, 1));
	    JavaPairRDD<String, Integer> counts = wordMap.reduceByKey((i1, i2) -> i1 + i2);
	    
	    List<Tuple2<String, Integer>> output = (List<Tuple2<String, Integer>>) counts.collect();

	    for (Tuple2<String, Integer> tuple : output) {
	      System.out.println(tuple._1() + ": " + tuple._2());
	    }
	    
	    spark.stop();
	}

}
