package com.sohu.video

import java.sql.{ResultSet, DriverManager}

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/9/22.
  */
object FilterWordCount {

  def main(args: Array[String]): Unit ={

    //设置运行环境
    val conf = new SparkConf().setAppName("FilterWordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 更换数据库配置
    val conn_str = "jdbc:mysql://localhost:3306/dreamore?user=root&password="
    // 建立连接
    val conn = DriverManager.getConnection(conn_str)
    conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    val divided = ArrayBuffer[String]()

    // 获取数据
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM CrowdfundingRelatedNews")

      // Iterate Over ResultSet
      while (rs.next) {
        //println(rs.getString("divided"))
        divided.+=(rs.getString("divided"))
      }

      sc.parallelize(divided)
        .flatMap(l => {
        val plist = l.split(",")
        val flag = 0
        for( str <- plist ) yield (str,1)
      }).filter(l => l._1.split("/").length == 2 && l._1.contains("n"))
        .coalesce(3, false)
        .map(l => (l._1.split("/")(0), 1))
        .reduceByKey(_+_)
        .repartition(1)
        .sortBy(l => l._2, false)
        .saveAsTextFile("/Users/zhaokangpan/IDEA/LrSgd/word_count_result")
    }
  }
}
