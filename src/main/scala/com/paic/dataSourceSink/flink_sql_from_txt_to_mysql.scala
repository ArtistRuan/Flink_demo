package com.paic.dataSourceSink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.types.logical.DateType

/**
  *
  * @program: FlinkEngine
  * @description: 通过FileSystem记载数据到输入表，并通过table api将数据直接加载到mysql
  *              数据已经可以跑通，但数据加载速度较慢，约30条每秒（收藏github上有批量加载数据，可能会优化速度，批加载待测）
  * @author: ruanshikao
  * @create: 2022-05-08 21:22
  *
  **/
object flink_sql_from_txt_to_mysql {
  def main(args: Array[String]): Unit = {

    // 获取流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 获取表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv.connect(new FileSystem().path("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\UserBehavior.dat"))
      .withFormat(new Csv()) // 文件是逗号分割，所以Csv格式
      .withSchema(new Schema()
      .field("web_id",DataTypes.STRING())
      .field("city_id",DataTypes.STRING())
      .field("user_id",DataTypes.STRING())
      .field("action_name",DataTypes.STRING())
      .field("action_time",DataTypes.BIGINT())
      )
      .createTemporaryTable("input_table") // 在环境中注册一张表

    val input_table: Table = tableEnv.from("input_table")

//    val sinkDDL:String = "create table webview_via_table_api_sink (\n "
//    + "web_id varchar(50),\n"
//    + "city_id varchar(50),\n"
//    + "user_id varchar(50),\n"
//    + "action_name varchar(50),\n"
//    + "action_time Bigint\n"
//    + ") with (\n"
//    + "'connector.type' = 'jdbc',\n"
//    + "'connector.url' = 'jdbc:mysql://localhost:3306/flink',\n"
//    + "'connector.table' = 'webview_via_table_api',\n"
//    + "'connector.driver' = 'com.mysql.jdbc.Driver',\n"
//    + "'connector.username' = 'root',\n"
//    + "'connector.password' = '123456')\n"

//    val sinkDDL = """
//        |create table webview_via_table_api_sink (
//        |  web_id varchar(50),
//        |  city_id varchar(50),
//        |  user_id varchar(50),
//        |  action_name varchar(50),
//        |  action_time Bigint
//        |) with (
//        | 'connector.type' = 'jdbc',
//        | 'connector.url' = 'jdbc:mysql://localhost:3306/flink',
//        | 'connector.table' = 'webview_via_table_api',
//        | 'connector.driver' = 'com.mysql.jdbc.Driver',
//        | 'connector.username' = 'root',
//        | 'connector.password' = '123456'
//        |)
//      """.stripMargin



    val sinkDDL:String = "create table webview_via_table_api_sink (web_id varchar(50),city_id varchar(50),user_id varchar(50),action_name varchar(50),action_time Bigint) with ('connector.type' = 'jdbc','connector.url' = 'jdbc:mysql://localhost:3306/flink','connector.table' = 'webview_via_table_api','connector.driver' = 'com.mysql.jdbc.Driver','connector.username' = 'root','connector.password' = '123456')"

    // 1 执行DDL建表；2 sink
    tableEnv.sqlUpdate(sinkDDL)

    input_table.insertInto("webview_via_table_api_sink")



    env.execute("table sql api")
  }
}
