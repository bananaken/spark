import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};

/**
  * Created by ken on 2016/6/20.
  */
case class irisdata(sepal_length: Double,sepal_width:Double,petal_length:Double,petal_width:Double)
object iris {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("abc").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:\\datamining\\iris\\*")
    val sqlcx = new SQLContext(sc)
    import sqlcx.implicits._

//    val dd = data.map(_.split(",")).map(p=>irisdata(p(0),p(1)))
//    val ddf = sqlcx.createDataFrame(dd,schema)
//    val df = dd.toDF("a","b","c","d","e")
//    df.show()
//    ddf.show()
  }
}
