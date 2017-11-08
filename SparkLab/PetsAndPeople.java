package hadoop.spark.redditanalyzer;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PetsAndPeople {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("PetsAndPeople")
				.enableHiveSupport()
				.getOrCreate();
		
		JavaRDD<String> petLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> peopleLines = spark.read().textFile(args[1]).javaRDD();
		
		JavaPairRDD<String, String> petMap = petLines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] parts = arg0.split(",");
				String petName = parts[0];
				String species = parts[1];
				return new Tuple2(petName, species);
			}
		});
		JavaPairRDD<String, String> peopleMap = peopleLines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				String[] parts = arg0.split(",");
				String personName = parts[0];
				String petName = parts[1];
				return new Tuple2(personName, petName);
			}
		});
		
		JavaPairRDD<String, Tuple2<String, String>> joined = petMap.join(peopleMap);
		
	    
	    List<Tuple2<String, Tuple2<String, String>>> output = (List<Tuple2<String, Tuple2<String, String>>>) joined.collect();

	    for (Tuple2<String, Tuple2<String, String>> tuple : output) {
	      System.out.println(tuple._1() + "," + tuple._2()._1() + "," + tuple._2()._2());
	    }
	    
	    spark.stop();
	}

}
