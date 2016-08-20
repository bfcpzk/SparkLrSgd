package com.sohu.video.lrsgd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 16/7/11.
  */
object FileTest {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LrSgd").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("/Users/zhaokangpan/Documents/sgd/test").flatMap( l => l.split(",")).map( l => l.toDouble )
    file.collect.foreach( l => println(l))
  }
}
