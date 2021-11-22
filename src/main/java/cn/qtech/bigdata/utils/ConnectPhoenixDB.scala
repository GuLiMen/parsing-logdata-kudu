package cn.qtech.bigdata.utils

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL
import cn.qtech.bigdata.comm.SendEMailWarning

object ConnectPhoenixDB {
  /**
   * 获取 phoenix 连接
   *
   * @param url
   * @return
   */
  def getConnection(url: String): Connection = {
    var conn: Connection = null
    try {
     // println("-------------------before the  Connection--------------------------")
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(url)
      println(s"----------------get  Connection----${conn.isClosed}---------------")
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s" parsing-logdata run Class ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")

      }

    }
    conn
  }


  /**
   * 关闭连接
   *
   * @param conn
   */
  def closeConn(conn: Connection): Unit = {
    try {
      if (conn != null) {
        conn.close()
        println(s"-----------close  Connection----${conn.isClosed}------------")
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"run Class ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")
      }
    }
  }
}
