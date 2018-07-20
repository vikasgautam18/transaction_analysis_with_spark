package com.wordpress.technicado

import com.wordpress.technicado.TransactionAnalysis.ExtractTrasactionFromInput
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class TransactionAnalysisTest extends FunSpec with BeforeAndAfterAll {

  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var inputRDD: RDD[String] = _
  var transactionRDD: RDD[(String, Int)] = _
  var genderRDD: RDD[(String, Int)] = _

  override def beforeAll() {
    // create spark context
    sparkConf = new SparkConf
    sparkConf.setMaster("local")
    sparkConf.setAppName("TransactionAnalysis_reduceByKeyTest")
    sparkContext = SparkContext.getOrCreate(sparkConf)

    inputRDD = sparkContext.parallelize(Seq("id,first_name,last_name,email,gender,age,transactions",
      "1,Albina,Hamal,ahamal0@yandex.ru,Female,39,14",
      "2,Cori,Kubasiewicz,ckubasiewicz1@mashable.com,Male,20,10",
      "3,Theadora,Oxterby,toxterby2@virginia.edu,Female,39,64",
      "4,Gaye,Myhill,gmyhill3@pinterest.com,Female,39,37",
      "5,Gil,Mugridge,gmugridge4@printfriendly.com,Male,20,35"))

    transactionRDD = TransactionAnalysis.ExtractTrasactionFromInput(inputRDD, rdd => {
      rdd.map(s => {
        val arr = s.split(",")
        (arr(5), arr(6).toInt)
      })
    })

    genderRDD = TransactionAnalysis.ExtractTrasactionFromInput(inputRDD, rdd => {
      rdd.map(s => {
        val arr = s.split(",")
        (arr(4), arr(6).toInt)
      })
    })
  }

  override def afterAll() {

    sparkConf = null
    sparkContext = null
    inputRDD = null

  }

  describe("TransactionAnalysis_reduceByKeyTest") {

    it("should Extract Age and Trasactions From Input RDD into a new RDD") {
      val expectedRDD : RDD[(String, Int)]= sparkContext.parallelize(Seq(("39", 14), ("20", 10), ("39", 64), ("39", 37), ("20", 35)))
      assertResult(expectedRDD.collect)(transactionRDD.collect)
    }

    it("should Find Average Transaction Per Age Group") {
      val expectedRDD: RDD[(String, Int)] = sparkContext.parallelize(Seq(("20", 22), ("39", 38)))
      val resultRDD: RDD[(String, Int)] = TransactionAnalysis.FindAverageTransactionPerGroup(transactionRDD)
      assertResult(expectedRDD.collect)(resultRDD.collect)
    }

    it("should Extract Gender and Trasactions From Input RDD into a new RDD") {
      val expectedRDD : RDD[(String, Int)]= sparkContext.parallelize(Seq(("Female", 14), ("Male", 10), ("Female", 64), ("Female", 37), ("Male", 35)))
      assertResult(expectedRDD.collect)(genderRDD.collect)
    }

    it("should Find Average Transaction Per Gender Group") {
      val expectedRDD: RDD[(String, Int)] = sparkContext.parallelize(Seq(("Male", 22), ("Female", 38)))
      val resultRDD: RDD[(String, Int)] = TransactionAnalysis.FindAverageTransactionPerGroup(genderRDD)
      assertResult(expectedRDD.collect)(resultRDD.collect)
    }

  }
}
