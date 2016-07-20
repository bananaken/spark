import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ken on 2016/7/7.
  */
case class bike(season: Int, yr: Int, mnth: Int, hr: Int, holiday: Int, weekday: Int, workingday: Int, weathersit: Int,
                temp: Double, atemp: Double, hum: Double, windspeed: Double, cnt: Int)

object bike_regession {

    // resCol保存处理后及不需要处理的特征名称集合，后面用于做特征合并
    var resCol = ArrayBuffer[String]()

    //批量index和oneHotEncoded，传入需要做处理的列名的array和dataFrame，输出一个处理后的dataFrame
    def indexAndOneHot(selectedCol:Array[String],rawTable:DataFrame):DataFrame={

        var temptable = rawTable
        val dualCol = ArrayBuffer[String]()

        for(i <- selectedCol){
            val indexInput = i
            val indexOutput = "index" + i
            val vecInput = indexOutput
            val vecOutput = "vec" + i

            val indexer = new StringIndexer()
                .setInputCol(indexInput)
                .setOutputCol(indexOutput)

            // val indexData = indexer.fit(rawTable).transform(rawTable)

            val encoder = new OneHotEncoder()
                .setInputCol(vecInput)
                .setOutputCol(vecOutput)
                .setDropLast(false)

            // val encodeData = encoder.transform(indexData)
            val pipeline = new Pipeline()
                .setStages(Array(indexer, encoder))

            temptable = pipeline.fit(temptable).transform(temptable)
            dualCol += vecOutput

        }
        resCol = dualCol ++ rawTable.columns.diff(selectedCol).filter(_!="cnt")
        return temptable
    }

    def main(args: Array[String]) {


        //初始化sparkContext和SQLContext
        val conf = new SparkConf().setAppName("abc").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlctx = new SQLContext(sc)
        import sqlctx.implicits._

        //读取数据并保存为dataframe格式
        val rawdata = sc.textFile("/user/root/bike/hour2.csv")
        val rawtable = rawdata.map(_.split(",")).map(x => bike(x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt,
            x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(16).toInt)).toDF

        //输入需要做oneHotCoder处理的列名，做oneHotcoder处理,用cache缓存数据
        val selectedCol = Array("season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "weathersit")
        val encodeData = indexAndOneHot(selectedCol,rawtable)
        encodeData.cache

        //进行特征合并RFormula
        val form = "cnt ~ " + resCol.mkString("+")
        val formula = new RFormula()
            .setFormula(form)
            .setFeaturesCol("features")
            .setLabelCol("label")
        val output = formula.fit(encodeData).transform(encodeData)
        output.select("features", "label").show()

        val Array(trainingData, testData) = output.randomSplit(Array(0.7, 0.3))

        // 进行linearRegression模型训练
        val lr = new LinearRegression()
            .setMaxIter(100)  //迭代计算次数
            .setRegParam(1)   //
            .setElasticNetParam(0) //0是L2,1是L1
            .setFeaturesCol("features")
            .setLabelCol("label")

        val lrModel = lr.fit(trainingData)
        val lrpredict = lrModel.transform(testData)

        // 进行linearRegression模型结果评估
        val lrEval = new RegressionEvaluator()
            .setPredictionCol("prediction")
            .setLabelCol("label")
            .setMetricName("mse")

        val lrEvalOut = lrEval.evaluate(lrpredict)

        // 决策树不需要做二元特征转换，直接从原特征进行组合
        val dtForm = "cnt~ " + rawtable.columns.filter(_!="cnt").mkString("+")
        formula.setFormula(dtForm)
            .setFeaturesCol("features")
            .setLabelCol("label")
        val dtOutput = formula.fit(rawtable).transform(rawtable)
        dtOutput.show(1)

        val Array(trainingDt, testDt) = dtOutput.randomSplit(Array(0.7, 0.3))

        // 决策树回归模型训练
        val dtRe = new DecisionTreeRegressor()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMaxDepth(10)

        val dtReModel = dtRe.fit(trainingDt)
        val dtRePredict = dtReModel.transform(testDt)
        // 决策树回归结果评估
        val DtReEval = new RegressionEvaluator()
            .setPredictionCol("prediction")
            .setLabelCol("label")
            .setMetricName("mse")

        val DtReEvalOut = DtReEval.evaluate(dtRePredict)

        //交叉验证线性回归
        val paramGrid = new ParamGridBuilder()
            .addGrid(dtRe.maxDepth, 3.to(10))
            .build()

        val cv = new CrossValidator()
            .setEstimator(dtRe)
            .setEvaluator(DtReEval)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(10)

        // cv.explainParams()
        val dtCVModel = cv.fit(trainingDt)
        val dtCVPredict = dtCVModel.transform(testDt)
        DtReEval.evaluate(dtCVPredict)
    }
}
