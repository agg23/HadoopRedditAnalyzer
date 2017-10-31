package edu.rosehulman.RedditAnalyzer

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

import scala.util.control.Breaks._

object WordScoreAverage {
   val NETWORK_WORD_COUNT = 20
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("WordAverageScore").getOrCreate()
    
    import spark.implicits._
    
    val rows = spark.read.orc("/Users/adam/Documents/01-9-2006.orc")
    val bodyRows = rows.select("body", "score")
        
    val dataset = bodyRows.as[(String, Int)]
    val wordsToScores = dataset.flatMap {
      case (body: String, score: Int) => body.toLowerCase().replaceAll("\\.", " ").split(" ").map((word: String) => (word.replaceAll("\\W", ""), score))
    }.toDF("word", "score")
    
    val wordStats = wordsToScores.filter {(row: Row) => row.getString(0) != ""}.groupBy("word").agg(count("word"), mean("score"))
    val words = wordStats.as[(String, BigInt, Double)].collect()
    
    val testComment = "Hello. I am an expert in computers and work in big data"
    val splitComment = testComment.toLowerCase().replaceAll("\\.", " ").split(" ").map((word: String) => word.replaceAll("\\W", ""))
    
    var splitCommentFrequencyIndex = 0
    // Word, Frequency, Overall word index
    var splitCommentFrequency = new Array[(String, BigInt, Int)](splitComment.length)
    
    // Fill array with empty tuples
    for(i <- 0 until splitCommentFrequency.length) {
      splitCommentFrequency(i) = ("", 0, 0)
    }
    
    var indicies = new Array[Int](NETWORK_WORD_COUNT)
    for(i <- 0 until words.length) {
      val row = words(i)
      val word = row._1
      
      breakable {
        for(j <- 0 until splitComment.length) {
          val commentWord = splitComment(j)
            
          if(commentWord == word) {
            splitCommentFrequency(splitCommentFrequencyIndex) = (commentWord, row._2, i)
            splitCommentFrequencyIndex += 1
          
            break
          }
        }
      }
    }
    
//    if(splitCommentFrequencyIndex != splitComment.length) {
//      // Not all words were in database
//    }
    
    println(f"$splitCommentFrequencyIndex%d words found from comment in subreddit dictionary")
    
    val sortedCommentFrequency = splitCommentFrequency.sortWith {(lhs, rhs) =>
      lhs._2 > rhs._2
    }
    
    println(sortedCommentFrequency.deep.mkString("\n"))
    
    wordStats.show()
    
    val layers = Array[Int](3, 10, 10, 1)
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers)
  }
}