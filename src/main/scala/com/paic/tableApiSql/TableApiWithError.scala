package com.paic.tableApiSql

import com.paic.kafkaData.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}


/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-04 00:46
  *
  **/
object TableApiWithError {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    tableApi(env)
    tableSql(env)
  }

  def tableApi(env:StreamExecutionEnvironment): Unit ={
    val textSourceStream: DataStream[String] = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt")
    val textDataStream = textSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1),arr(2).toDouble)
      }
    )

    textDataStream.print()

    // environment configuration
    val settings = EnvironmentSettings
      .newInstance()
//      .useOldPlanner()
      .inStreamingMode()
      .build()

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    //基于数据流创建表
    val dataTable: Table = tableEnv.fromDataStream(textDataStream)
    //调用table api进行转换
    val resultTable: Table = dataTable.select("sensorName,recordTime,temperature").filter("sensorName == 'sensor_1'")
    //table不能直接输出，转为追加数据流输出
    resultTable.toAppendStream[(String,String,Double)].print("result")
    resultTable.printSchema()

    env.execute("table api")
  }

  def tableSql(env:StreamExecutionEnvironment): Unit ={
    val textSourceStream: DataStream[String] = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt")
    val textDataStream = textSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1),arr(2).toDouble)
      }
    )
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //基于数据流创建表
    val dataTable: Table = tableEnv.fromDataStream(textDataStream)
    //用sql做转换
    tableEnv.createTemporaryView("datasTable",dataTable)
    //定义sql
    val sql:String = "select sensorName,recordTime,temperature from datasTable where sensorName = 'sensor_2'"

    val resultTableSql: Table = tableEnv.sqlQuery(sql)
    resultTableSql.toAppendStream[(String,String,Double)].print()

    env.execute("table sql")
  }


}
