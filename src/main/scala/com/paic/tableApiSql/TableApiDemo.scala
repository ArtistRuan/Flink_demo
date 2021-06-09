package com.paic.tableApiSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._


/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-04 15:15
  *
  **/
object TableApiDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    stardTable(env)
  }
  //1 基本老版本planner的流处理
//  def oldVerPlanner(env:StreamExecutionEnvironment): Unit ={
//    val settings = EnvironmentSettings
//      .newInstance()
//      .useOldPlanner()
//      .build()
//    //1流处理
//    val oldStreamTableEnv = StreamTableEnvironment.create(env)
//    //2批处理
//    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val oldbatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
//
//    //3基于blink流处理
//    val blinkSettings = EnvironmentSettings
//      .newInstance()
//      .inStreamingMode()
//      .useBlinkPlanner()
//      .build()
//    val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,blinkSettings)
//
//    //4基于blink的批处理
//    val batchBlinkEnvSettings = EnvironmentSettings
//      .newInstance()
//      .inBatchMode()
//      .useBlinkPlanner()
//      .build()
//    val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(batchBlinkEnvSettings)
//
//  }

  //标准定义表
  def stardTable(env:StreamExecutionEnvironment): Unit ={
    val inputPath = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tEnv.connect( new FileSystem().path(inputPath))
//      .withFormat(new OldCsv) //因为OldCsv已经被弃用，不符合RC文件规范，用新的csv
      .withFormat(new Csv)
      .withSchema(new Schema().field("sensor_name",DataTypes.STRING())
      .field("timestamp",DataTypes.BIGINT())
      .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")

    val table: Table = tEnv.from("inputTable")

    table.toAppendStream[(String,Long,Double)].print()

    env.execute("table")

  }

}
