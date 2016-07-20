import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.evaluation.RegressionEvaluator
/**
  * Created by ken on 2016/7/4.
  */
object CF_test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("abc").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlctx = new SQLContext(sc)
    import sqlctx.implicits._
    val rawdata = sc.textFile("D:\\ml-100k\\u.data")
    val raw_table = rawdata.map(_.split("\t")).map(x=>Rating(x(0).toInt,x(1).toInt,x(2).toDouble)).toDF
    val model = new ALS()
      .setRank(10)
      .setRegParam(0.01)
       .setMaxIter(10)
       .setItemCol("product")
       .setUserCol("user")
       .setRatingCol("rating")
    val alsmodel = model.fit(raw_table)
    val predict = alsmodel.transform(raw_table)
    val recomm = predict.where("user=234").orderBy($"prediction".desc).show(5)

    //mse评估
    val predictval = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      .setMetricName("mse")
    predictval.evaluate(predict)

  }
}

