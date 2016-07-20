import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ken on 2016/5/20.
  */


object helloworld {
  def max(x:Int,y:Int,z:Int):Int={
    var m = 0
    var k = 0
    if(x>y)
      k=x
    else
      k>z
    if(k>z)
      m=k
    else
      m=z
    return m

  }
  def main(args: Array[String]): Unit = {
    val d = max(2,1,3)
    println(d)
  }
}
