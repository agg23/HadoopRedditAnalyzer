package edu.rosehulman.RedditAnalyzer

import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

import scala.util.control.Breaks._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.hive.HiveContext

object WordScorePredictor {
   val COMMENT_LIMIT = 100000
   val TRAINING_COMMENT_COUNT = 2000
   val NETWORK_WORD_COUNT = 20
  
  def main(args: Array[String]): Unit = {
    var subreddit = ""
    var comment = ""
    if(args.length == 1) {
      subreddit = ""
      comment = args(0)
    } else if(args.length == 2) {
      subreddit = args(0)
      comment = args(1)
    } else {
      println("Usage: wordscore (subreddit) [comment]")
    }
     
//    val spark = SparkSession.builder().master("local").appName("WordScorePredictor").getOrCreate()
    // Config for use in spark-submit
    val spark = SparkSession.builder().appName("WordScorePredictor").config("spark.sql.warehouse.dir", "/apps/hive/warehouse").enableHiveSupport().getOrCreate()
    
    import spark.implicits._
    
    println("Initialization complete. Selecting rows")
    
    spark.sql("USE Reddit")
    
    var rows = if(subreddit != "") spark.sql(s"SELECT body, score FROM Comments WHERE subreddit_id == '$subreddit'") else spark.sql("SELECT body, score FROM Comments")
    
//    val rows = spark.read.orc("/Users/adam/Documents/01-9-2006.orc")
    val bodyRows = rows.select("body", "score").limit(COMMENT_LIMIT)
        
    val dataset = bodyRows.as[(String, Int)]
    // Map words to their scores
    val wordsToScores = dataset.flatMap {
      case (body: String, score: Int) => body.toLowerCase().replaceAll("\\.", " ").split(" ").map((word: String) => (word.replaceAll("\\W", ""), score))
    }.toDF("word", "score")
    
    // Count the frequency of words, and aggregate their scores
    val wordStats = wordsToScores.filter {(row: Row) => row.getString(0) != ""}.groupBy("word").agg(count("word"), mean("score"))
    val wordsDataset = wordStats.as[(String, BigInt, Double)]
    
//    wordsDataset.write.format("csv").save("/tmp/spark/words")
    
    val words = wordsDataset.collect()
    
    // Get n comments for training the network
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
      
    val trainer = new LinearRegression().setFeaturesCol("features").setLabelCol("_1")
    
    val model = trainer.fit(transformedTrainingComments)
    
    println("Training completed")
        
    val formattedCommentTuple = commentIndicies(words, comment) match {
      case Array(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) => (0, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
    }
    
    val commentDataset = List(formattedCommentTuple).toDS()
    
    val transformedCommentDataset = featureAssembler.transform(commentDataset)
    
    transformedCommentDataset.show()
    
    val finalResult = model.transform(transformedCommentDataset)
    finalResult.show()
    
    finalResult.select("prediction").write.format("csv").save("/tmp/spark/output")
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