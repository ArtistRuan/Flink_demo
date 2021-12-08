package com.paic.tableApiSql

import com.paic.kafkaData.SensorName
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-06 15:45
  *
  **/
object SinkMysql {
  def main(args: Array[String]): Unit = {
    /**
      * SINK TO RELATIONSHIP DB SUCH AS MYSQL
      */
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义时间特性：1 处理时间 2 事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //从文件获取数据
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor_count.txt"
    val textSource: DataStream[String] = env.readTextFile(input)

    //封装
    val source: DataStream[SensorName] = textSource.map(SensorName(_))

    source.print()

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//    val table: Table = tableEnv.fromDataStream(source) //直接整个表，下面为指定字段名
    val table: Table = tableEnv.fromDataStream(source,'sensorName)
    tableEnv.createTemporaryView("sourceTable",table)
    //etl
    val sql = "select sensorName,count(sensorName) as cnt from sourceTable group by sensorName "
    val sourceTable: Table = tableEnv.sqlQuery(sql)

//    //查看执行计划
//    val explain: String = tableEnv.explain(sourceTable)
//    println(explain)
    //定义输出mysql数据
    val sinkDDL =
      """
        |create table jdbcOutputTable (
        | sensorName varchar(20) not null,
        | cnt bigint not null
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://localhost:3306/test',
        | 'connector.table' = 'sensor_count',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = '123456'
        |)
      """.stripMargin
    //sink:1执行DDL建表；2sink
    tableEnv.sqlUpdate(sinkDDL)
    sourceTable.insertInto("jdbcOutputTable")

    //execute
    env.execute("sink to mysql")

  }
}
