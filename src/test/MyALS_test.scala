package test

import org.apache.log4j._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import model.MyALS

object MyALS_test {

  def _rddStr2rddRatings(rddStr: RDD[String]): RDD[Rating] =
	rddStr.map(_.split(',') match { case Array(user, item, rate, ts) =>
	  Rating(user.toInt, item.toInt, rate.toDouble)
	})

//  def _printRecsForUser()
//
//  def _printRecsForProducts()

  def main(args: Array[String]): Unit = {
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

	val sc = new SparkContext(new SparkConf().setAppName("ALS").setMaster("local[*]"))

	// read data from HDFS and delete header line
	var rawRatings = sc.textFile("hdfs://localhost:9000/input/ml_data_small/ratings.csv")
	  .zipWithIndex().filter(_._2 >= 1).keys

	var rawMovies = sc.textFile("hdfs://localhost:9000/input/ml_data_small/movies.csv")
	  .zipWithIndex().filter(_._2 >= 1).keys

	// split raw dataset into training set and testing set and transform to RDD[Rating]
	val randomSeq = rawRatings.randomSplit(Array(0.7, 0.3))
	val train = _rddStr2rddRatings(sc.parallelize(randomSeq(0).collect(), numSlices=100))
	val test = _rddStr2rddRatings(sc.parallelize(randomSeq(1).collect(), numSlices=100))


//	val myALS = new MyALS()
//
//	myALS.fit(train)
//	myALS.printRecs(myALS.getModel.recommendProductsForUsers(3), myALS.id2Name(rawMovies))
	//    println(train.take(3).mkString("Array(", ", ", ")"))
	//    println(test.take(3).mkString("Array(", ", ", ")"))
	//    println(ratings.take(3).mkString("Array(", ", ", ")"))
  }
}
