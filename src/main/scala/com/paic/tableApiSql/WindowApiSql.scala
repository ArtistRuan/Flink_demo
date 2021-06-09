package com.paic.tableApiSql

import com.paic.kafkaData.{SensorReading, SensorTem}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  *
  * @program: FlinkEngine
  * @description: Window corporation with Api & SQL
  * @author: ruanshikao
  * @create: 2021-06-07 10:13
  *
  **/
object WindowApiSql {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,60000L))
//    env.enableCheckpointing(1000L,CheckpointingMode.EXACTLY_ONCE)

    //定义数据源
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceTextStream: DataStream[String] = env.readTextFile(input)

    //封装数据源
    val sourceStream: DataStream[SensorTem] = sourceTextStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
          override def extractTimestamp(element: SensorTem): Long = element.timestamp * 1000L
        })

    //调用执行
//    printSchema(env,sourceStream)
//    groupWindowApi(env,sourceStream)
    groupWindowSql(env,sourceStream)
    /**
      * 5和4执行有点问题
      */
//    overWindowApi(env,sourceStream)
//    overWindowSql(env,sourceStream)
  }
  //PRINT SCHEMA
  def printSchema(env:StreamExecutionEnvironment,sourceStream:DataStream[SensorTem]): Unit ={
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable = tableEnv.fromDataStream(sourceStream,'id,'timestamp.rowtime as 'ts,'temperature)

    sourceTable.printSchema()
    sourceTable.toAppendStream[Row].print()

    env.execute()
  }
  //1 groupWindowApi
  def groupWindowApi(env:StreamExecutionEnvironment,sourceStream:DataStream[SensorTem]): Unit ={
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp.rowtime as 'ts,'temperature)

    //每10秒统计一次
    val apiResult = sourceTable
        .window(Tumble over 10.seconds on 'ts as 'tw)
        .groupBy('id,'tw)
        .select('id,'id.count,'temperature.avg,'tw.end) //'tw.end 关窗时间

    apiResult.toAppendStream[Row].print("window api")

    env.execute()
  }

  //2 groupWindowSql
  def groupWindowSql(env:StreamExecutionEnvironment,sourceStream:DataStream[SensorTem]): Unit ={
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp.rowtime as 'ts,'temperature)
    tableEnv.createTemporaryView("sourceTable",sourceTable)
    //id:String,timestamp:Long,temperature:Double
    //每10秒统计一次
    val sqlResult = tableEnv.sqlQuery(
      """
        |select
        | id,count(id),avg(temperature),
        | tumble_end(ts,interval '10' second)
        |from
        | sourceTable
        |group by
        | id,
        | tumble(ts,interval '10' second)
      """.stripMargin)

    sqlResult.toAppendStream[Row].print("sqlResult")

    env.execute()
  }
  //3 over window api
  def overWindowApi(env:StreamExecutionEnvironment,sourceStream:DataStream[SensorTem]): Unit ={
    //定义表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)

    //统计每个sensor每条数据，与之前两行数据的平均温度
    val apiResult = sourceTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id,'ts,'id.count over 'ow,'temperature.avg over 'ow)

    apiResult.toAppendStream[Row].print()

    env.execute()
  }
  //4 over window sql
  def overWindowSql(env:StreamExecutionEnvironment,sourceStream:DataStream[SensorTem]): Unit ={
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)
    tableEnv.createTemporaryView("sourceTable",sourceTable)

    val sqlRessult = tableEnv.sqlQuery(
      """
        |select
        | id,
        | ts,
        | count(id) over ow,
        | avg(temperature) over ow
        |from sourceTable
        |window ow as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
      """.stripMargin)

    sqlRessult.toAppendStream[Row].print()

    env.execute()
  }
}
