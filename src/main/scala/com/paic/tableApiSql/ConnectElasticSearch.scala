package com.paic.tableApiSql

import com.paic.kafkaData.SensorReading
import com.sun.prism.PixelFormat.DataType
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-06 12:55
  *
  **/
object ConnectElasticSearch {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    fileStreamElasticsearch(env)
    fileTableElasticsearch(env)

  }

  //flink api 基于文件流数据创建表
  def fileStreamElasticsearch(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val textSourceStream: DataStream[String] = env.readTextFile(input)

    //封装成sensorReading
    val sourceStream = textSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1),arr(2).toDouble)
      }
    )

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //基于数据流创建表
    val table: Table = tableEnv.fromDataStream(sourceStream)
    tableEnv.createTemporaryView("sourceTable",table)

    //etl
    val sql = "select sensorName,count(sensorName) cnt from sourceTable group by sensorName"
    val inputSource = tableEnv.sqlQuery(sql)
    //定义输出的es
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("192.168.174.200",9200,"http")
      .index("sensor")
      .documentType("sensor_temperature")
    )
      .inUpsertMode()
      .withFormat( new Json() )
      .withSchema(new Schema()
        .field("sensor_name",DataTypes.STRING())
        .field("cnt",DataTypes.BIGINT())
      )
      .createTemporaryTable("es_sink_table")

    inputSource.insertInto("es_sink_table")

    env.execute("file -> table -> es")
  }

  //flink table 基于文件表数据创建表
  def fileTableElasticsearch(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.connect(new FileSystem().path(input)
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
          .field("sensor_name",DataTypes.STRING())
          .field("timestampStr",DataTypes.BIGINT())
          .field("temperature",DataTypes.DECIMAL(15,2))
      )
      .createTemporaryTable("inputTable")

    //定义sql
    val sql = "select sensor_name,count(sensor_name) cnt from inputTable group by sensor_name"
    val inputTable: Table = tableEnv.sqlQuery(sql)

    //定义输出es_output_sink_table
    tableEnv.connect(new Elasticsearch()
        .version("6")
        .host("192.168.174.200",9200,"http")
        .index("sensor")
        .documentType("sensor_temperature")
    )
      .inUpsertMode()
      .withFormat( new Json() )
        .withSchema( new Schema()
            .field("sensorName",DataTypes.STRING())
            .field("cnt",DataTypes.BIGINT())
        )
        .createTemporaryTable("es_output_sink_table")

    inputTable.insertInto("es_output_sink_table")

    env.execute("flink table file sink es")
  }
}
