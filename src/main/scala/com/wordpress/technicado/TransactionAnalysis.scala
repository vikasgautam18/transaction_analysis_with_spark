package com.wordpress.technicado

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import com.wordpress.technicado.Constants._
import org.apache.spark.rdd.RDD

object TransactionAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length != 0){
      println("USAGE: spark-submit --class com.wordpress.technicado.movielens.RatingCategories_withCountByValue " +
        "--master local[*] jars/movielens_analysis_2.11-0.1.jar ")
      System.exit(-1)
    }

    //read necessary properties
    Utils.readConfig("conf/transactions.properties")

    // create spark context
    val sparkConf = new SparkConf
    sparkConf.setAppName(Utils.getString(TA_SPARK_APP_NAME))
    val sparkContext = new SparkContext(sparkConf)

    //read input data into RDD

    val inputRDD: RDD[String] = sparkContext.textFile(Utils.getString(TA_INPUT_FILE_PATH))

    val tupleRDD: RDD[(String, Int)] = ExtractTrasactionFromInput(inputRDD)

    // group by age and find average of transaction per age group
    val resultRDD: RDD[(String, Int)] = FindAverageTransactionPerAgeGroup(tupleRDD)

    // print the results
    println("the average transactions per age group is ::: ")
    resultRDD.collect().sortBy(_._2).foreach(println)

  }

  def FindAverageTransactionPerAgeGroup(tupleRDD: RDD[(String, Int)]) = {
    val groupedRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    val resultRDD: RDD[(String, Int)] = groupedRDD.mapValues(iter => {
      iter.reduce(_ + _) / iter.toList.length
    })
    resultRDD
  }

  def ExtractTrasactionFromInput(inputRDD: RDD[String]) = {

    //remove header
    val header = inputRDD.first()
    val transactionRDD = inputRDD.filter(row => row != header)

    //debugging
    println("Input sample as read from source file ::: ")
    transactionRDD.take(10).foreach(println)

    // get rid of unwanted columns in the dataset
    val tupleRDD: RDD[(String, Int)] = transactionRDD.map(s => {
      val arr = s.split(",")
      (arr(5), arr(6).toInt)
    })
    tupleRDD
  }
}
