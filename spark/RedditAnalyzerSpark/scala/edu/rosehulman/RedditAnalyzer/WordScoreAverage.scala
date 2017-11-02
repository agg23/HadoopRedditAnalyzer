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
    
    
//    if(splitCommentFrequencyIndex != splitComment.length) {
//      // Not all words were in database
//    }
    println(commentIndicies(words, testComment).deep.mkString("\n"))
    
    wordStats.show()
    
    val layers = Array[Int](3, 10, 10, 1)
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers)
  }
   
   def commentIndicies(words: Array[(String, BigInt, Double)], comment: String): Array[Int] = {
    val commentFrequencyTuple = commentFrequency(words, comment)
    val sortedCommentFrequency = commentFrequencyTuple._1
    val splitCommentFrequencyIndex = commentFrequencyTuple._2
    
    var indicies = new Array[Int](NETWORK_WORD_COUNT)
    for(i <- 0 until indicies.length) {
      // Fill indicies with 0
      indicies(i) = 0
    }
    
    println(f"$splitCommentFrequencyIndex%d words found from comment in subreddit dictionary")
        
    if(splitCommentFrequencyIndex < NETWORK_WORD_COUNT) {
      // All words are passed into the network
      for(i <- 0 until splitCommentFrequencyIndex) {
        indicies(i) = sortedCommentFrequency(i)._3
      }
    } else {
      for(i <- 0 until NETWORK_WORD_COUNT/2) {
        // Select highest frequency n/2
        indicies(i) = sortedCommentFrequency(i)._3
        
        // Select lowest frequency n/2
        indicies(NETWORK_WORD_COUNT - 1 - i) = sortedCommentFrequency(splitCommentFrequencyIndex - 1 - i)._3
      }
    }
    
    println(sortedCommentFrequency.deep.mkString("\n"))
    println(indicies.deep.mkString("\n"))
    
    return indicies
   }
   
   def commentFrequency(words: Array[(String, BigInt, Double)], comment: String): (Array[(String, BigInt, Int)], Int) = {
    val splitComment = comment.toLowerCase().replaceAll("\\.", " ").split(" ").map((word: String) => word.replaceAll("\\W", ""))
     
    var splitCommentFrequencyIndex = 0
    // Word, Frequency, Overall word index
    var splitCommentFrequency = new Array[(String, BigInt, Int)](splitComment.length)
    
    // Fill array with empty tuples
    for(i <- 0 until splitCommentFrequency.length) {
      splitCommentFrequency(i) = ("", 0, 0)
    }
        
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
    
    val sortedCommentFrequency = splitCommentFrequency.sortWith {(lhs, rhs) =>
      lhs._2 > rhs._2
    }
    
    return (sortedCommentFrequency, splitCommentFrequencyIndex)
   }
}