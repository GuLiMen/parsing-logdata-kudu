package cn.qtech.bigdata.utils

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL
import cn.qtech.bigdata.comm.SendEMailWarning

object ConnectionImpala{
  /**
   * 获取 Impala 连接
   *
   * @param url
   * @return
   */
  def getConnection(url: String): Connection = {
    var conn: Connection = null
    try {
      println("------------before connect----------------------")
      Class.forName("com.cloudera.impala.jdbc41.Driver")
      conn = DriverManager.getConnection(url)
      println(s"------------get connect ${conn.isClosed}----------------------")

    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")

      }

    }
    conn
  }

  /**
   * 获取 Impala 连接
   * @param hosts
   * @param port
   * @return
   */
  def getConnection(hosts: util.ArrayList[String] ,port:Int,database:String): Connection = {
    if(hosts.size() ==0){
      println("没有impala主机地址")
      return null
    }
    val impalaUrl = "jdbc:impala://"+hosts.get(0)+":"+port+"/"+database+";UseSasl=0;AuthMech=3;UID=qtkj;PWD="

    var conn: Connection = null
    try {
      println("------------before connect----------------------")
      Class.forName("com.cloudera.impala.jdbc41.Driver")
      conn = DriverManager.getConnection(impalaUrl)
      println(s"------------get connect ${conn.isClosed}----------------------")
    } catch {
      case e: Exception => {
        println(e.getMessage)
        e.printStackTrace()
        //  SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")
        println(hosts.get(0)+" impala服务下线")
        for(i <- 1 until hosts.size()){
          try{
            val impalaUrl2 = "jdbc:impala://"+hosts.get(i)+":"+port+"/"+database+";UseSasl=0;AuthMech=3;UID=qtkj;PWD="
            println("------------before connect----------------------")
            val  tempoConn = DriverManager.getConnection(impalaUrl2)
            if(tempoConn!=null){
              conn=tempoConn
            }
          }catch {
            case e: Exception => e.printStackTrace()
          }
        }
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
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"run Class ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")
      }
    }
  }
}
