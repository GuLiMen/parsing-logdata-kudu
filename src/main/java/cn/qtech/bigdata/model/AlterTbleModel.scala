package cn.qtech.bigdata.model

import java.util

import scala.annotation.meta.{beanGetter, beanSetter}

@beanGetter@beanSetter
class AlterTbleModel {


  var THsql: String = _
  var CBsql: String = _
  var GCsql: String = _
  var notExitsCloumn: util.ArrayList[String] = null

  def getTHsql: String = this.THsql

  def setTHsql(THsql: String): Unit = this.THsql = THsql

  def getCBsql: String = this.CBsql

  def setCBsql(CBsql: String): Unit = this.CBsql = CBsql

  def getGCsql: String = this.GCsql

  def setGCsql(GCsql: String): Unit = this.GCsql = GCsql


  def getNotExitsCloumn: util.ArrayList[String] = this.notExitsCloumn

  def setNotExitsCloumn(notExitsCloumn: util.ArrayList[String]): Unit = this.notExitsCloumn = notExitsCloumn
}
