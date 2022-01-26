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

class MyALS(private val rank: Int = 30,
			private val iterations: Int = 10,
			private val lambda: Double = 0.1,
			private val blocks: Int = -1,
			private val seed: Long = System.nanoTime()) {

  private var model: Option[MatrixFactorizationModel] = None

  def getModel: MatrixFactorizationModel = model.get

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

  def _calPrecision(sc: SparkContext, len: Long, dict: RDD[(Set[Int], Set[Int])]): Double = {
	val acc = sc.doubleAccumulator("precisionAccumulator")
	dict.foreach(
	  t =>
		acc.add(t._2.intersect(t._1).size.toDouble / t._2.size)
	)
	acc.sum / len
  }

  def _calRecall(sc: SparkContext, len: Long, dict: RDD[(Set[Int], Set[Int])]): Double = {
	val acc = sc.doubleAccumulator("recallAccumulator")
	dict.foreach(
	  t =>
		acc.add(t._2.intersect(t._1).size.toDouble / t._1.size)
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

	var itemSet: mutable.Set[Int] = mutable.Set()
	var recItemSet: mutable.Set[Int] = mutable.Set()

	recItemAcc.value.forEach(
	  t => recItemSet = recItemSet ++ t
	)

	itemAcc.value.forEach(
	  t => itemSet = itemSet ++ t
	)
	recItemSet.size.toDouble / itemSet.size
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

  /** *
   * 预测，返回推荐列表，格式为
   * [
   * [UserId, [ItemIdList]\],
   * ...
   * ]
   *
   * @param recNum 推荐的数量
   */
  def predict(recNum: Int): RDD[(Int, Iterable[Int])] = {

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
	println("Predicting completed!")
	res
  }

  /** *
   * 比对测试集和推荐结果的差异，给出详细数据
   *
   * @param sc             SparkContext 实例
   * @param train          训练集数组
   * @param test           测试集数组
   * @param predictionDict 推荐的用户 - [物品]字典
   * @param _allItemIDs    所有物品的 ID 列表
   * @return 包含 RMSE、AUC 和 coverage 的 Map
   */
  def score(sc: SparkContext,
			train: RDD[Rating],
			test: RDD[Rating],
			predictionDict: RDD[(Int, Iterable[Int])],
			_allItemIDs: Array[Int]): mutable.Map[String, Double] = {

	val resMap: mutable.Map[String, Double] = mutable.Map()

	// transform data
	val trainDict = _getUserItemDict(train).cache()
	val testDict = _getUserItemDict(test).cache()
	val allItemIDs = sc.broadcast(_allItemIDs)

	val dict = testDict.join(predictionDict).sortByKey().values.map(
	  t =>
		(t._1.toSet, t._2.toSet)
	).cache()

	val testLength = test.count()
	val predLength = predictionDict.count()

	resMap += "RMSE" -> _calRMSE(test, predictionDict)
//	resMap += "precision" -> _calPrecision(sc, predLength, dict)
//	resMap += "recall" -> _calRecall(sc, testLength, dict)
	resMap += "coverage" -> _calCoverage(sc, trainDict, testDict, predictionDict)
	resMap += "AUC" -> _calAUC(test, allItemIDs)

	trainDict.unpersist()
	testDict.unpersist()
	dict.unpersist()

	resMap
  }
}