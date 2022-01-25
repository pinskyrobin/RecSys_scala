package scala.com.demo.model

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** *
 *
 * @param rank       隐向量长度
 * @param iterations ALS 算法迭代次数
 * @param lambda     正则系数
 * @param blocks     并行计算的分块数量
 * @param seed       随机初始化矩阵的种子
 */

class MyALS(private val rank: Int = 20,
			private val iterations: Int = 10,
			private val lambda: Double = 0.1,
			private val blocks: Int = -1,
			private val seed: Long = System.nanoTime()) {

  private var model: Option[MatrixFactorizationModel] = None
  //  private var bestModel: Option[MatrixFactorizationModel] = None

  /** *
   * 输出有含义的推荐列表
   *
   * @param recs   推荐列表
   * @param mapper 物品 id 到名字的映射
   */
  def _printRecs(recs: RDD[(Int, Array[Int])], mapper: mutable.Map[Int, String]): Unit =
	recs.foreach {
	  t =>
		print(s"\r\n=== Recommend for USER${t._1} ===\r\n")
		for (i <- t._2.indices) {
		  print(s"ITEM$i: ${mapper(t._2(i))}\r\n")
		}
	}

  def _getUserItemDict(data: RDD[Rating]): RDD[(Int, Iterable[Int])] =
	data.map(
	  t => (t.user, t.product)
	).groupByKey().sortByKey()

  def _calRMSE(test: RDD[Rating], predictionDict: RDD[(Int, Iterable[Int])]): Double = {
	// ratings for each pair "user -> product"
	val testRate = test.map(
	  t => ((t.user, t.product), t.rating)
	)

	val predictionRate = model.get.predict(
	  predictionDict.flatMapValues(t => t)
	).map(
	  t => ((t.user, t.product), t.rating)
	)

	val ratings = predictionRate.join(testRate).values
	math.sqrt(ratings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / test.count())
  }

  def _calPrecision(sc: SparkContext, len: Long, dict: RDD[(Seq[Int], Seq[Int])]): Double = {
	val acc = sc.doubleAccumulator("precisionAccumulator")
	dict.foreach(
	  t =>
		acc.add(t._2.intersect(t._1).length.toDouble / t._2.length)
	)
	acc.sum / len
  }

  def _calRecall(sc: SparkContext, len: Long, dict: RDD[(Seq[Int], Seq[Int])]): Double = {
	val acc = sc.doubleAccumulator("recallAccumulator")
	dict.foreach(
	  t =>
		acc.add(t._2.intersect(t._1).length.toDouble / t._2.length)
	)
	acc.sum / len
  }

  def _calCoverage(sc: SparkContext, trainDict: RDD[(Int, Iterable[Int])], testDict: RDD[(Int, Iterable[Int])], predictionDict: RDD[(Int, Iterable[Int])]): Double = {
	val itemAcc: CollectionAccumulator[Set[Int]] = sc.collectionAccumulator("itemAccumulator")
	val recItemAcc: CollectionAccumulator[Set[Int]] = sc.collectionAccumulator("recItemAccumulator")

	val item = trainDict.union(testDict)

	item.foreach(
	  t => {
		itemAcc.add(t._2.toSet)
	  }
	)

	predictionDict.foreach(
	  t =>
		recItemAcc.add(t._2.toSet)
	)

	recItemAcc.value.toArray.distinct.length.toDouble / itemAcc.value.toArray.distinct.length
  }

  def _calAUC(test: RDD[Rating], _allItemIDs: Broadcast[Array[Int]]): Double = {

	val positiveUserProducts = test.map(
	  r =>
		(r.user, r.product)
	)

	val positivePredictions = model.get.predict(positiveUserProducts).groupBy(_.user)

	val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
	  userIDAndPosItemIDs => {
		val random = new Random()
		userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
		  val posItemIDSet = posItemIDs.toSet
		  val negative = new ArrayBuffer[Int]()
		  val allItemIDs = _allItemIDs.value
		  var i = 0
		  // keep about as many negative examples per user as positive.
		  while (i < allItemIDs.length && negative.size < posItemIDSet.size) {
			val itemID = allItemIDs(random.nextInt(allItemIDs.length))
			if (!posItemIDSet.contains(itemID)) {
			  negative += itemID
			}
			i += 1
		  }
		  // a collection of (user,negative-item) tuples
		  negative.map(itemID => (userID, itemID))
		}
	  }
	}.flatMap(t => t)

	// make predictions on the rest:
	val negativePredictions = model.get.predict(negativeUserProducts).groupBy(_.user)

	positivePredictions.join(negativePredictions).values.map {
	  case (positiveRatings, negativeRatings) =>

		var correct = 0L
		var total = 0L

		for (positive <- positiveRatings;
			 negative <- negativeRatings) {

		  // count the correctly-ranked pairs
		  if (positive.rating > negative.rating) {
			correct += 1
		  }
		  total += 1
		}

		correct.toDouble / total
	}.mean()
  }

  def fit(ratingsRDD: RDD[Rating]): Unit = {
	// transform csv data into Rating data
	val ratings = ratingsRDD.cache()
	model = Some(ALS.train(ratings, rank, iterations, lambda, blocks, seed))
	ratings.unpersist()
	println("Training completed!")
  }


  //  /** *
  //   * 自动寻找一定范围内的最优参数组合
  //   *
  //   * @param ratings 评分 RDD 数组
  //   */
  //  def autoFit(ratings: RDD[Rating]): Unit = {
  //	println("This may process for a long time, please wait patiently.")
  //	println("Start exploring best parameters...")
  //
  //	val randomSeq = ratings.randomSplit(Array(0.6, 0.4))
  //	val training = randomSeq(0)
  //	val validation = randomSeq(1)
  //
  //	val _ranks = List(5, 30)
  //	val _iterations = List(5, 20)
  //	val _lambdas = List(0.01, 3.00)
  //	var bestRank = 0
  //	var bestIterations = 0
  //	var bestLambda = 0.0
  //	var bestScore = 100.0
  //
  //	for (_rank <- _ranks; _iteration <- _iterations; _lambda <- _lambdas) {
  //	  model = Option(ALS.train(training, _rank, _iteration, _lambda))
  //	  val score = _calRMSE(validation, predict(80))
  //	  if (score < bestScore) {
  //		bestScore = score
  //		bestRank = _rank
  //		bestIterations = _iteration
  //		bestLambda = _lambda
  //		bestModel = model
  //		println("**Parameter updated!**")
  //		println("Optimized Score: " + bestScore)
  //		println("Optimized Rank: " + bestRank)
  //		println("Optimized Iterations: " + bestIterations)
  //		println("Optimized Lambda: " + bestLambda)
  //	  }
  //	}
  //
  //	println("BestScore: " + bestScore)
  //	println("BestRank: " + bestRank)
  //	println("BestIterations: " + bestIterations)
  //	println("BestLambda: " + bestLambda)
  //
  //	ratings.unpersist()
  //  }

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
  def predict(recNum: Int, trans: Boolean = false, mapper: mutable.Map[Int, String] = null): RDD[(Int, Iterable[Int])] = {

	var recs: RDD[(Int, Array[Rating])] = null

	try {
	  recs = model.get.recommendProductsForUsers(recNum)
	} catch {
	  case e: NoSuchElementException =>
		println("!!! Please train the model before predicting !!!")
		e.printStackTrace()
		return null
	}

	val res = recs.map(
	  rec =>
		(rec._1, rec._2.map(t => t.product).toIterable)
	)
	//TODO:trans部分
	println("Predicting completed!")
	res
  }

  /** *
   * 比对测试集和推荐结果的差异，给出详细数据
   *
   * @param test           测试集数组
   * @param predictionDict 推荐的用户 - [物品]字典
   * @return 包含 precision、recall 和 coverage 的 Map
   */
  def score(sc: SparkContext, train: RDD[Rating],
			test: RDD[Rating],
			predictionDict: RDD[(Int, Iterable[Int])],
			_allItemIDs: Array[Int]): mutable.Map[String, Double] = {
	val resMap: mutable.Map[String, Double] = mutable.Map()

	// transform data
	val trainDict = _getUserItemDict(train)
	val testDict = _getUserItemDict(test)
	val allItemIDs = sc.broadcast(_allItemIDs)

	val dict = testDict.join(predictionDict.sortByKey()).values.map(
	  t =>
		(t._1.toSeq, t._2.toSeq)
	)
	val testLength = test.count()
	val predLength = predictionDict.count()

	resMap += "RMSE" -> _calRMSE(test, predictionDict)
	resMap += "precision" -> _calPrecision(sc, predLength, dict)
	resMap += "recall" -> _calRecall(sc, testLength, dict)
	resMap += "coverage" -> _calCoverage(sc, trainDict, testDict, predictionDict)
	resMap += "AUC" -> _calAUC(test, allItemIDs)

	resMap
  }
}