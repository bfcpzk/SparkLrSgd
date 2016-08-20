package com.sohu.video.lrsgd
import java.sql.{ DriverManager, ResultSet}

/**
  * Created by zhaokangpan on 16/7/12.
  */
object JdbcTest {

  def main(args: Array[String]) {
    // Change to Your Database Config
    val conn_str = "jdbc:mysql://localhost:3306/sparkweibo?user=root&password="

    // Load the driver
    //classOf[com.mysql.jdbc.Driver]

    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM relation LIMIT 5")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getString("u_a_id"))
      }
    }
  }
}
