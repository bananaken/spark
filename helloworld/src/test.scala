import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner
/**
  * Created by ken on 2016/5/10.
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("abc").setMaster("local")
    val sc = new SparkContext(conf)
    //val text = sc.parallelize(List("160301033111647,BestPayGD@001.ctnbc-bon.net;201603010000000001,四川,2016-03-01 00:05,魔力小鸟(北京）信息技术有限公司,91Y游戏--91Y游戏-30000金币,18111187637,5,支付失败,帐户余额不足"))
    //val text = sc.parallelize(Seq("a","cc","aa","ll"))
    //val text = sc.textFile("D:\\code\\spark\\helloworld\\test_loan.txt")
    //text.collect()
    //val c = text.flatMap(_.split(",")).foreach().map(word=>(word,1)).reduceByKey(_+_).sortBy(_._2,ascending = false)
    //println(text.replaceFirst(",[^,]*",""))
    //c.foreach(println)
    //val result = text.map(_.replaceFirst(",[^,]*","")).saveAsTextFile()
    //val c = text.flatMap(_.split(","))
    val a = sc.parallelize(List((1,2),(3,4),(3,6)))
    val b = sc.parallelize(List((3,9)))
    
  }
}
