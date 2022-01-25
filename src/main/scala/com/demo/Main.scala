package scala.com.demo

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
	val sc = new SparkContext(new SparkConf().setAppName("ALS").setMaster("local[*]"))
	val map = List(1->Iterable(2,3,4), 2->Iterable(5,6,7), 6->Iterable(10,11))
	val rdd = sc.parallelize(map)
	println(rdd.flatMapValues(t => t).collect().mkString("Array(", ", ", ")"))
  }
}