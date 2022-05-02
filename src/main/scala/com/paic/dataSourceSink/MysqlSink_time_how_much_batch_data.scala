package com.paic.dataSourceSink

import java.net.URL

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * 这份代码可以用于将数据下沉到mysql
  */


/**
  *
  * @program: FlinkEngine
  * @description: 将数据sink到mysql
  * @author: ruanshikao
  * @create: 2021-05-26 00:11
  *
  **/


object MysqlSink_time_how_much_batch_data {
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val driverClass = "com.mysql.jdbc.Driver"
    val dbUrl = "jdbc:mysql://localhost:3306/flink"
    val userNmae = "root"
    val passWord = "123456"
    val sql = "insert into flink.webview (web_id,city_id,user_id,action_name,action_time) values (?,?,?,?,?)"
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val sourcePath: URL = getClass.getResource("/UserBehavior.dat")
    val source_data: DataSet[String] = env.readTextFile(sourcePath.getPath())

    /**
      * 每秒处理的数据量依然是30条数据左右，50万数据4.5小时（16460秒）
      */
    source_data.map(new MapFunction[String,Row] {
      override def map(line: String): Row = {
        // 将字符串转为Row，用于JDBCOut到数据库
        val arr: Array[String] = line.split(",")
        val row = new Row(5)
        row.setField(0,arr(0))
        row.setField(1,arr(1))
        row.setField(2,arr(2))
        row.setField(3,arr(3))
        row.setField(4,arr(4))
        row
      }
    })

    .output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(driverClass)
      .setDBUrl(dbUrl)
      .setUsername(userNmae)
      .setPassword(passWord)
      .setQuery(sql)
      .finish()).setParallelism(4)

    env.execute("JDBC output")

    val end_time: Long = System.currentTimeMillis()

    val execute_time = (end_time - start_time) / 1000
    println("执行时间：" + execute_time)
  }
}