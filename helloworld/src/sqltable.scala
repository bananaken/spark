import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ken on 2016/5/10.
  */
case class Person(name:String,age:Int)

object sqltable {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("abc").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlctx = new SQLContext(sc)
    import sqlctx.implicits._
    //case class people()
    //val df = sc.textFile("D:\\code\\spark\\helloworld\\test_loan.txt").map(_.split(","))

    //val df = sqlctx.read.text("D:\\code\\spark\\helloworld\\test_loan.txt")


//    val people = sc.textFile("D:\\people.txt").map(_.split(",")).filter(_.length == 2).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    println(people)
////    println(people.first().age)
//    people.registerTempTable("people")
//    val result = sqlctx.sql("SELECT name, age FROM people")
////    result.show()
////    result.collect().foreach(println)
////    result.map(t => "Name: " + t(0)).collect().foreach(println)
//    result.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
//    println(people.schema)
//
//    val ds = Seq(1, 2, 3).toDS()
//    ds.map(_ + 1).collect()
//    val df = ds.toDF()
//    val df2 = df
//    df.join(df2,"left_outer")
//    df.na.fill(0)
//    df.write.format("orc").mode("append").save()
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.mllib.linalg.Vectors

    // Crates a DataFrame
    val dataset: DataFrame = sqlctx.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(dataset)

    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
    model.transform(dataset)
  }
}
