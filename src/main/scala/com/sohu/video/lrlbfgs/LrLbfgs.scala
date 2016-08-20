package com.sohu.video.lrlbfgs

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/7/11.
  */
object LrLbfgs {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LrLbfgs")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    //读取数据,稀疏存储
    val data = sc.textFile(args(0)).map(line => {
      val parts = line.split(",")
      val index = ArrayBuffer[Int]()
      val value = ArrayBuffer[Double]()
      for( i <- 1 until parts.length ){
        if(parts(i).toDouble != 0.0){
          index.+=(i-1)
          value.+=(parts(i).toDouble)
        }
      }
      LabeledPoint(parts(0).toDouble, Vectors.sparse(1280, index.toArray, value.toArray))
    }).cache()

    //数据集抽样
    var count = 0
    val pos_data = data.filter( l => l.label == 1)
    val neg_data = data.filter( l => l.label == 0).map( l => {
      count += 1
      (l, count)
    }).filter( l => l._2 < 4000).map( l => l._1)

    val pos_splits = pos_data.randomSplit(Array(0.7, 0.3), seed = 10L)
    val pos_training = pos_splits(0).cache()
    val pos_test = pos_splits(1)

    val neg_splits = neg_data.randomSplit(Array(0.7, 0.3), seed = 10L)
    val neg_training = neg_splits(0).cache()
    val neg_test = neg_splits(1)

    val train_data = pos_training.union(neg_training)
    val test_data = pos_test.union(neg_test)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train_data)

    //为了计算AUC需要将分类sigmoid映射函数去除掉
    model.clearThreshold

    // 在测试集上计算得分
    val scoreAndLabels = test_data.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    //获得评分矩阵.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC

    //将权重结果按照,分割
    val resultString = model.weights.toArray.mkString(",")
    //日期转换
    def getNowDate( flag : Int ) : String={
      val now : Date = new Date()
      var dateFormat : SimpleDateFormat = null
      if(flag == 1){
        dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
      }else{
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      }
      val hehe = dateFormat.format( now )
      hehe
    }
    val tt = getNowDate(1)
    //结果存储为一个文件,共分为4行
    sc.parallelize(List("auRoc",auROC,"weight",resultString), 1).saveAsTextFile(s"hdfs://rccluster1/user/nlp/warehouse/model/LRSGD/sparklr/result/sgd_${args(3)}_$tt")

    // 设置连接地址
    val conn_str = "jdbc:mysql://10.13.83.24:3306/dm_recommmend?user=nlp&password=123456"
    // 建立数据库连接
    val conn = DriverManager.getConnection(conn_str)
    conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    // 插入数据
    try {
      val prep = conn.prepareStatement("insert into t_gbdt_sgd_version (gbdt_key, sgd_key, gbdt_version, sgd_version, auc, create_time, update_time ) VALUES (?, ?, ?, ?, ?, ?, ?) ")
      prep.setString(1, "GBDT:MODEL:ON")
      prep.setString(2, "LR:SGDMODEL:ON")
      prep.setString(3, "Spark#GBDT_" + args(3))
      prep.setString(4, "Spark#sgd_" + args(3) + "_" + tt)
      prep.setDouble(5, auROC)
      prep.setString(6, getNowDate(2))
      prep.setString(7, getNowDate(2))
      prep.executeUpdate
    }
  }
}