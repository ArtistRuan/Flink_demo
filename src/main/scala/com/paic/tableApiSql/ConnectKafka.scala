package com.paic.tableApiSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-05 15:41
  *
  **/
object ConnectKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    connectKafka(env)
  }

  //从kafka获取数据
  def connectKafka(env:StreamExecutionEnvironment): Unit ={
    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    streamTableEnv.connect(new Kafka()
        .version("0.10")
        .topic("sensor")
        .property("zookeeper.connect","192.168.174.100:2181,192.168.174.110:2181,192.168.174.120:2181")
        .property("bootstrap.servers","192.168.174.100:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
      .field("name",DataTypes.STRING())
      .field("timestampStr",DataTypes.BIGINT())
      .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafka_table")

    val kafka_table: Table = streamTableEnv.from("kafka_table")

    //将表转为流来打印
    kafka_table.toAppendStream[(String,Long,Double)].print()

    //sqlQuery方法一
    val sqlTable: Table = streamTableEnv.sqlQuery("select name,temperature from kafka_table")
    sqlTable.toAppendStream[(String,Double)].print("sqlTable1")

    //sqlQuery方法二(区分大小写)
    val sqlTable2 = streamTableEnv.sqlQuery(
      """
        |select name,timestampStr from kafka_table
      """.stripMargin)
    sqlTable2.toAppendStream[(String,Long)].print("sqlTable2")


    env.execute("kafka_source_table")
  }
}
