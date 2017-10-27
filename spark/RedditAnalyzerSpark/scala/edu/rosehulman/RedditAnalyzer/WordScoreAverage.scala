package edu.rosehulman.RedditAnalyzer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

object WordScoreAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("WordAverageScore").getOrCreate()
    
    import spark.implicits._
    
    val rows = spark.read.orc("/Users/adam/Documents/01-9-2006.orc")
    val bodyRows = rows.select("body", "score")
        
    val dataset = bodyRows.as[(String, Int)]
    val wordsToScores = dataset.flatMap {
      case (body: String, score: Int) => body.toLowerCase().split(" ").map((word: String) => (word.replaceAll("\\W", ""), score))
    }.toDF("word", "score")
    
    val wordStats = wordsToScores.groupBy("word").agg(count("word"), mean("score"))
    
    wordStats.show()
    
    val layers = Array[Int](3, 10, 10, 1)
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers)
  }
}