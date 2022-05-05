package com.paic.tableApiSql

import java.net.URL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: 读取文本数据，解析完毕后入库mysql，代码尚未调通20220505 23:55
  * @author: ruanshikao
  * @create: 2022-05-05 22:44
  *
  **/
case class web_login_cc (web_id:String,city_id:String,user_id:String,action_name:String,action_time:Long)
object web_login_from_text_to_mysql {
  def main(args: Array[String]): Unit = {
    // 创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取数据文件
    val data_path: URL = getClass().getResource("/UserBehavior.dat")
    val source_stream: DataStream[String] = env.readTextFile(data_path.getPath)

    // 解析数据
    val parsed_stream: DataStream[web_login_cc] = source_stream.map(
      data => {
        val arr: Array[String] = data.split(",")
        web_login_cc(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
      }
    )


    // 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 将数据源映射到表
    val source_table: Table = tableEnv.fromDataStream(parsed_stream)
//    val source_table: Table = tableEnv.fromDataStream(parsed_stream,
//      ['web_id,
//      'city_id,
//      'user_id,
//      'action_name,
//      'action_time]
//    )

    // 注册临时表,表名为path = 里面的 source_table
    tableEnv.createTemporaryView("source_table",source_table)

    // 查看10条数据
    val target_sql = "select web_id, city_id, user_id, action_name, action_time from source_table"
    val source_target_table: Table = tableEnv.sqlQuery(target_sql)

    // 查看执行计划
    val sql_explain: String = tableEnv.explain(source_target_table)
    println("执行计划为：" + "\n" + sql_explain)

    // 定义输出对应表（严格限制不可以使用关键字，如default，table等）
    val sinkDDL =
      """
        |create table webview_via_table_api (
        |  web_id varchar(50),
        |  city_id varchar(50),
        |  user_id varchar(50),
        |  action_name varchar(50),
        |  action_time varchar(50)
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://localhost:3306/flink',
        | 'connector.table' = 'webview_via_table_api',
        | 'connector.username' = 'root',
        | 'connector.password' = '123456'
        |)
      """.stripMargin

    // 1 执行DDL建表；2 sink
    tableEnv.sqlUpdate(sinkDDL)
    source_target_table.insertInto("webview_via_table_api")

    // 启动任务
    env.execute("table api execute flink from text to mysql via table api")


  }
}
