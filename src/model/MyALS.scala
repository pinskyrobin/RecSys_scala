package model

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import shapeless.ops.tuple.Mapper

import scala.collection.Map


/** *
 *
 * @param rank       隐向量长度
 * @param iterations ALS 算法迭代次数
 * @param lambda     正则系数
 * @param blocks     并行计算的分块数量
 * @param seed       随机初始化矩阵的种子
 */

class MyALS(private val rank: Int = 20,
			private val iterations: Int = 5,
			private val lambda: Double = 0.01,
			private val blocks: Int = -1,
			private val seed: Long = 0) {

  private var model: Option[MatrixFactorizationModel] = None

  //TODO: 测试完毕后需要删除
  def getModel: MatrixFactorizationModel = model.get


  //  def printRecs(recs: RDD[(Int, Array[Rating])], mapper: Map[Int, String]): Unit =
  //	recs.foreach {
  //	  t =>
  //		print(s"\r\n=== Recommend for USER${t._1} ===\r\n")
  //		for (i <- t._2.indices) {
  //		  print(s"ITEM$i: ${t._2(i).product}, SCORE: ${t._2(i).rating}\r\n")
  //		}
  //	}

  /** *
   * 输出有含义的推荐列表
   *
   * @param recs   推荐列表
   * @param mapper 物品 id 到名字的映射
   */
  def printRecs(recs: RDD[(Int, Array[Int])], mapper: Map[Int, String]): Unit =
	recs.foreach {
	  t =>
		print(s"\r\n=== Recommend for USER${t._1} ===\r\n")
		for (i <- t._2.indices) {
		  print(s"ITEM$i: ${mapper(t._2(i))}\r\n")
		}
	}

  /** *
   * 将 id 通过 RDD 转换为映射
   *
   * @param mapper RDD 数组
   * @return id - Name 的映射关系
   */
  def id2Name(mapper: RDD[String]): Map[Int, String] =
	mapper.flatMap {
	  line =>
		val items = line.split(',')
		if (items(0).isEmpty) None
		else Some((items(0).toInt, items(1)))
	}.collectAsMap()

  def fit(ratingsRDD: RDD[Rating]): Unit = {
	// transform csv data into Rating data
	val ratings = ratingsRDD.cache()
	model = Some(ALS.train(ratings, rank, iterations, lambda, blocks, seed))
	ratings.unpersist()
  }


  /** *
   * 自动寻找一定范围内的最优参数组合
   *
   * @param ratings 评分 RDD 数组
   */
  def autoFit(ratings: RDD[Rating]): Unit = {
	println("This may process for a long time, please wait patiently.")
	println("Start exploring best parameters...")

	val randomSeq = ratings.randomSplit(Array(0.7, 0.3))
	val train = randomSeq(0).collect()
	val validation = randomSeq(1).collect()

	var _ranks = List(5, 30)
	var _iterations = List(5, 20)
	var _lambdas = List(0.01, 3.00)
	var bestRank = 0
	var bestIterations = 0
	var bestLambda = 0.0

	for (_rank <- _ranks; _iteration <- _iterations; _lambda <- _lambdas) {
	  var model = ALS.train(ratings, rank, iterations, lambda, blocks, seed)
	  //TODO: compute RMSE and update
	}

	ratings.unpersist()
  }

  /** *
   * 返回推荐列表，格式为
   * [
   * [UserId, [ItemIdList]\],
   * ...
   * ]
   *
   * @param recNum 推荐的数量
   * @param trans  是否返回物品 id 对应的名字
   */
  def predict(recNum: Int, trans: Boolean = false, mapper: Mapper[Int, String] = null): RDD[(Int, Array[Int])] = {
	val recs = model.get.recommendProductsForUsers(recNum)
	val res = recs.map(
	  rec =>
		(rec._1, rec._2.map(t => t.product))
	)
	res
  }

  /** *
   * 比对测试集和推荐结果的差异，给出详细数据
   *
   * @param test        测试集数组
   * @param predictions 推荐数组
   */
  def score(test: RDD[Rating], predictions: RDD[Rating]): Map[String, Double] = {
	var resMap: Map[String, Double] = null
	val testRate = test.map(
	  t => ((t.user, t.product), t.rating)
	)
	val predictionRate = model.get.predict(
	  predictions.map(
		t => (t.user, t.product)
	  )
	).map(
	  t => ((t.user, t.product), t.rating)
	)

	// RMSE
	val ratings = predictionRate.join(testRate).values
	val RMSE = math.sqrt(ratings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / test.count())
	resMap += ("RMSE" -> RMSE)

	// precision
	val testDict = _getUserItemDict(test)
	val predictionDict = _getUserItemDict(predictions)
	val _dict = testDict.join(predictionDict).values
	val dict = _dict.map(
	  t => (t._1.toSeq, t._2.toSeq)
	)
	val testLength = test.count()
	val predLength = predictions.count()
	var precision = 0.0

	dict.map(
	  t =>
		precision += (t._2.length - t._2.diff(t._1).length) / predLength
	)

	// recall
	var recall = 0.0

	dict.map(
	  t =>
		recall += (t._2.length - t._2.diff(t._1).length) / testLength
	)

	// coverage

	resMap
  }

  def _getUserItemDict(data: RDD[Rating]): RDD[(Int, Iterable[Int])] =
	data.map(
	  t => (t.user, t.product)
	).groupByKey().sortByKey()

}