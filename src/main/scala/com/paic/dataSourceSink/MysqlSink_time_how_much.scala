package com.paic.dataSourceSink

/**
  * 这份代码可以用于将数据下沉到mysql
  */

import java.net.URL
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._


/**
  *
  * @program: FlinkEngine
  * @description: 将数据sink到mysql，采用upsert模式
  * @author: ruanshikao
  * @create: 2021-05-26 00:11
  *
  **/

// 定义数据接收样例类
case class WebView(web_id:String,city_id:String,user_id:String,action_name:String,action_time:Long)
object MysqlSink_time_how_much {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(4)
    val start_time: Long = System.currentTimeMillis()
    dataSinkMysql(env)
    val end_time: Long = System.currentTimeMillis()

    val execute_time = (end_time - start_time) / 1000
    println("执行时间：" + execute_time)


  }

  def dataSinkMysql(env:StreamExecutionEnvironment): Unit ={
    // 有 543462 条数据
//    val sourcePath: URL = getClass.getResource("/UserBehavior.csv")  // csv每秒30条数据入库 4个并行度
    val sourcePath: URL = getClass.getResource("/UserBehavior.dat")  // dat每秒30条数据入库 4个并行度
    val web_datastreaming: DataStream[String] = env.readTextFile(sourcePath.getPath)

    val dataSource: DataStream[WebView] = web_datastreaming.map(
      data => {
        val arr: Array[String] = data.split(",")
        WebView(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
      }
    ).setParallelism(4)

    //print
//    dataSource.print("数据集")
    dataSource.addSink(new MyJdbcSinkFunc()).setParallelism(4)

    env.execute("Data Sink To Mysql")

  }
}

class MyJdbcSinkFunc() extends RichSinkFunction[WebView]{
  // 定义连接信息
  var conn:Connection = _
  var insertStmt:PreparedStatement = _

  override def invoke(value: WebView, context: SinkFunction.Context[_]): Unit = {
    insertStmt.setString(1,value.web_id)
    insertStmt.setString(2,value.city_id)
    insertStmt.setString(3,value.user_id)
    insertStmt.setString(4,value.action_name)
    insertStmt.setLong(5,value.action_time)
    insertStmt.execute()
  }

  override def open(parameters: Configuration): Unit = {
    // 定义驱动
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink","root","123456")
    // 定义预编译语句
    insertStmt = conn.prepareStatement("insert into webview values (?,?,?,?,?)")
  }

  override def close(): Unit = {
    conn.close()
    insertStmt.close()
  }
}