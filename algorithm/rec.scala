import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

sc.setCheckpointDir("hdfs://emr-header-1.cluster-65423:9000/tmp/checkpointdir");
val data_click = sc.textFile("hdfs://emr-header-1.cluster-65423:9000/58pic-rec/*-click1.txt")
val data_down = sc.textFile("hdfs://emr-header-1.cluster-65423:9000/58pic-rec/*-down1.txt")
val data_favo = sc.textFile("hdfs://emr-header-1.cluster-65423:9000/58pic-rec/*-favo1.txt")
//val data = sc.textFile("oss://rd-emr/58pic_rec/58pic-rec-down*,oss://rd-emr/58pic_rec/58pic-rec-favo*",100)

//val ratings = data.map(_.split("\t")).filter(t => t(3).toLong > 1522512000 && t(3).toLong < 1523030400 ).map(x => ((x(0),x(1)),x(2))).reduceByKey((x,y) => (x.toDouble+y.toDouble).toString).map(_ match { case ((x, y), z) => Rating(x.toInt, y.toInt, z.toDouble)}).cache()
val ratings_click = data_click.map(_.split("\t")).map(x => ((x(0),x(1)),1.0))
val ratings_down = data_down.map(_.split("\t")).map(x => ((x(0),x(1)),3.0))
val ratings_favo = data_favo.map(_.split("\t")).map(x => ((x(0),x(1)),2.0))
val ratings = ratings_click.union(ratings_down).union(ratings_favo).reduceByKey((x,y) => x.toDouble+y.toDouble).map(_ match { case ((x, y), z) => Rating(x.toInt, y.toInt, z.toDouble)}).cache()

val ratings_click = data_click.map(_.split("\t")).map(x => (x(0),x(1))).groupByKey().map(x => (x_1,x_2,x_2.count))

ratings.checkpoint
ratings.first

val allusers = ratings.map(_.user).distinct()
val allproducts = ratings.map(_.product).distinct()
println("Got "+ratings.count()+" ratings from "+allusers.count+" users on "+allproducts.count+" products.")

//val splits = ratings.randomSplit(Array(0.8, 0.2), seed = 111l)
//val training = splits(0)
//val test = splits(1)

// 2. 训练模型
val rank = 12
val lambda = 0.01
val numIterations = 100
val model = ALS.train(ratings, rank, numIterations, lambda)

model.save(sc,"hdfs://emr-header-1.cluster-65423:9000/tmp/model-6-04-100")
allusers.saveAsTextFile("hdfs://emr-header-1.cluster-65423:9000/tmp/users-6-04")
allproducts.saveAsTextFile("hdfs://emr-header-1.cluster-65423:9000/tmp/products-6-04")

val model = MatrixFactorizationModel.load(sc,"hdfs://emr-header-1.cluster-65423:9000/tmp/model-all-100")
model.userFeatures.repartition(100)
model.userFeatures.cache()
model.userFeatures.count
model.productFeatures.repartition(100)
model.productFeatures.cache()
model.productFeatures.count

import org.jblas.DoubleMatrix

/* Compute the cosine similarity between two vectors */
def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
  vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
}

val K = 9
val result = allproducts.collect.flatMap { x =>
  val itemFactor = model.productFeatures.lookup(x).head
  val itemVector = new DoubleMatrix(itemFactor)
  val sims = model.productFeatures.map{ case (id, factor) =>
    val factorVector = new DoubleMatrix(factor)
    val sim = cosineSimilarity(factorVector, itemVector)
    (id, sim)
  }
  val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
  (sortedSims)
}

val usersProducts= test.map { case Rating(user, product, rate) =>
  (user, product)
}
usersProducts.count  //Long = 1000209

var predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
  ((user, product), rate)
}
predictions.count //Long = 1000209

val ratesAndPreds = test.map { case Rating(user, product, rate) =>
  ((user, product), rate)
}.join(predictions)
ratesAndPreds.count  //Long = 1000209


val rmse= math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean())

println(s"RMSE = $rmse")



spark.driver.memory=5G
  spark.driver.memoryOverhead=1G
  spark.executor.memory=5G
  spark.executor.memoryOverhead=1G
  spark.executor.cores=6
spark.sql.shuffle.partitions = 2500

spark-shell --master yarn  --deploy-mode client --executor-memory 9G --driver-memory 3G --executor-cores 5 --num-executors 4

