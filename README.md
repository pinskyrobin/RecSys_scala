## MLlib ALS

### 算法评测

评测以 RMSE、precision、recall 和 coverage 四个方面衡量。

### 问题记录

#### RDD 算子中变量修改失效问题

程序运行至计算覆盖率时，出现如下错误：

```scala
java.lang.ArithmeticException: / by zero

	at scala.com.demo.model.MyALS._calCoverage(MyALS.scala:104)
	at scala.com.demo.model.MyALS.score(MyALS.scala:190)
	at scala.com.demo.MyALS_test.test01(MyALS_test.scala:60)
	...
```

首先在错误附近打断点，监测变量值。

在 `item.foreach(t => itemSet.++(t._2.toSet))` 里， `item` 可以正常取到值，但 `itemSet` 集合始终为空。

另外，将添加操作更换为打印操作，系统可以输出 `t._2` 的值，因此推测是添加集合元素的方式不正确，导致 `itemSet.size` 返回 0，出现除零错误。

继续排查，发现两集合定义时使用了不可变的 `val`，遂将其改为 `var`. 但更改完成后依然报错。于是放弃连接集合，采用对集合内元素单独操作的方式。

```scala
item.foreach(
  t =>
   t._2.foreach(
     e =>
      itemSet + e
   )
)
```

依然报错！！！

为分母添加平滑项 1，使得分母不为 0，此时成功运行，得到的结果 precision、recall 和 coverage 均为0， 说明计算前两项时同样出错。

后续先后尝试：

1. 使用 `Iterable` 类型定义 `itemSet` 和 `recItemSet`，直到计算时再使用 `.toSet` 转换为集合，失败
2. 将项目设置涉及到 JDK 的选项全部设定为 1.8，失败
3. 将 `foreach` 改为普通的 for 循环，失败
4. 更新 IDEA 以及 scala 插件版本，失败
5. 不使用 `JUnit` 框架，使用普通的 object 测试，失败
6. 在 for 循环内，将 ` dict.foreach(t =>num += t._2.toSet)` 更改为自增一个常量值 ` dict.foreach(t =>num += 1)`，检查错误是否出现在自增项，失败

最后，尝试将 `Dict()` 更改为简单的 `List()`，

```scala
List(1, 2, 2, 432, 5, 345, 34, 56, 34, 6).foreach(
  t => num += t
)
```

结果不为 0，说明是**迭代对象 RDD**出现了问题。

查阅资料，Spark 算子内的变量实际上是外部变量的副本，对其修改不能改变外部的变量。这是因为 RDD 是分布在多个机器上的，其算子也是如此，普通变量不能简单的在各个机器中同时修改。因此需要借助累加器 `Accumulator`，顾名思义该变量只能增加，只有 `driver` 能获取到 `Accumulator` 的值（使用 `value` 方法），Task 只能对其做增加操作。

对计算 precision、recall 和 coverage 分别使用 `DoubleAccumulator` 和 `CollectionAccumulator`，得到了正确的结果。

#### 除法问题

考虑如下输出：

```
println(1 / 3)
println(1.0 / 3)
println(1.toLong / 3)
println(1.toDougle / 3)
```

第一个和第三个均输出 0，第二个和第四个有理想输出 0.3333...，因此计算时应使用 `.toDouble` 来确保除法的正确性。

![image-20220125110743816](C:\Users\x50021862\AppData\Roaming\Typora\typora-user-images\image-20220125110743816.png)





