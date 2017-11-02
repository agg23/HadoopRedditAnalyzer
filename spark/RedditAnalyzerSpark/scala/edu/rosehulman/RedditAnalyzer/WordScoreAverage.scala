package edu.rosehulman.RedditAnalyzer

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

import scala.util.control.Breaks._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object WordScoreAverage {
   val TRAINING_COMMENT_COUNT = 2000
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
    
    // Get n comments for training the network
//    val trainingComments = dataset.limit(TRAINING_COMMENT_COUNT).collect()
//    var trainingIndicies = new Array[Array[Int]](TRAINING_COMMENT_COUNT)
//    var trainingScores = new Array[Int](TRAINING_COMMENT_COUNT)
//    for(i <- 0 until trainingComments.length) {
//      val row = trainingComments(i)
//      trainingIndicies(i) = commentIndicies(words, row._1)
//      trainingScores(i) = row._2
//    }
    
    val trainingComments = dataset.limit(TRAINING_COMMENT_COUNT).map { case (body: String, score: Int) => 
      val splitWords = body.toLowerCase().replaceAll("\\.", " ").split(" ")
      
      commentIndicies(words, body) match {
        case Array(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => (score, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
      }
    }
    
    trainingComments.show()
    
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9", "_10", "_11", "_12", "_13", "_14", "_15", "_16", "_17", "_18", "_19", "_20", "_21"))
      .setOutputCol("features")
      
    val labelAssembler = new VectorAssembler().setInputCols(Array("_1")).setOutputCol("scores")
      
    val transformedTrainingComments = featureAssembler.transform(trainingComments).select("_1", "features")
    
    transformedTrainingComments.show()
      
//    val layers = Array[Int](1, 1, 1, 1)
    val trainer = new LinearRegression().setFeaturesCol("features").setLabelCol("_1")
    
    val model = trainer.fit(transformedTrainingComments)
    
    println("Training completed")
    
//    model.transform(dataset)
    
    val comment = "Hello. I am an expert in computers and work in big data"
    
    val formattedCommentTuple = commentIndicies(words, comment) match {
      case Array(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => (0, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
    }
    
    val commentDataset = List(formattedCommentTuple).toDS()
    
    val transformedCommentDataset = featureAssembler.transform(commentDataset)
    
    transformedCommentDataset.show()
    
    model.transform(transformedCommentDataset).show()
    
//    if(splitCommentFrequencyIndex != splitComment.length) {
//      // Not all words were in database
//    }
//    println(commentIndicies(words, testComment).deep.mkString("\n"))
    
//    wordStats.show()
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
    
//    println(f"$splitCommentFrequencyIndex%d words found from comment in subreddit dictionary")
        
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
    
//    println(sortedCommentFrequency.deep.mkString("\n"))
//    println(indicies.deep.mkString("\n"))
    
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