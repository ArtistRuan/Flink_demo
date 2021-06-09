package com.paic.tableApiSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-06 00:02
  *
  **/
object KafkaPipeline {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //创建输入表
    tableEnv.connect(new Kafka()
        .version("0.10")
        .topic("sensor")
        .property("zookeeper.connect","192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181")
        .property("bootstrap.servers","192.168.174.100:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
          .field("name",DataTypes.STRING())
          .field("timestampStr",DataTypes.STRING())
          .field("temperature",DataTypes.STRING())
      )
      .createTemporaryTable("inputKafkaTable")

    val inputKafkaTable: Table = tableEnv.from("inputKafkaTable")
    //流式agg操作不能输出到kafka
//    val inputTable = inputKafkaTable.select('timestampStr,'temperature)
//      .groupBy('timestampStr)
//      .select('timestampStr,'temperature.count as 'cnt)
    //简单处理1
//    val inputTable: Table = inputKafkaTable.select('timestampStr,'temperature)
//    //简单处理2
//    val inputTable = tableEnv.sqlQuery(
//      """
//        |select timestampStr,temperature from inputKafkaTable
//      """.stripMargin)

    //简单处理3
    val inputTable = tableEnv.sqlQuery(
      """
        |select timestampStr,cast(temperature as decimal(15,2)) * 100 from inputKafkaTable
      """.stripMargin)

    //创建输出表
    tableEnv.connect(new Kafka()
        .version("0.10")
        .topic("kafka_sink")
        .property("zookeeper.connect","192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181")
        .property("bootstrap.servers","192.168.174.100:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
          .field("timestamp",DataTypes.STRING())
          .field("temperature",DataTypes.DECIMAL(15,2))
      )
      .createTemporaryTable("outputKafkaTable")

    //pipeline
    inputTable.insertInto("outputKafkaTable")


    env.execute("from kafka to kafka")

  }
}
