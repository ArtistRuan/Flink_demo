package com.paic.tableApiSql

import com.paic.kafkaData.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-05 19:38
  *
  **/
object DataStream_Table {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从文件获取数据做dataStream
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceData: DataStream[String] = env.readTextFile(input)
    //将数据用sensorReading样例类封装起来
    val sourceDataStream = sourceData.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1),arr(2).toDouble)
      }
    )
    //创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //获取数据
    val inputTable: Table = tableEnv.fromDataStream(sourceDataStream)
    //定义输出output
    val output = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\output2.txt"
    //定义输出表
    tableEnv.connect(new FileSystem().path(output))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("name",DataTypes.STRING())
        .field("timestamp",DataTypes.STRING())
        .field("temperture",DataTypes.DOUBLE())
      )
      .createTemporaryTable("output_table")

    inputTable.insertInto("output_table")

    env.execute("dataStream to table to file")

  }
}
