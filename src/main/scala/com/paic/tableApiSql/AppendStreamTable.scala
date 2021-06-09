package com.paic.tableApiSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-05 18:51
  *
  **/
object AppendStreamTable {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    /**
      * 需求： 读取文件内容到表，再将文件输出到文件
      */
    //1创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //2定义输入source
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    //3用source注册表
    tableEnv.connect( new FileSystem().path(input))
      .withFormat( new Csv )
      .withSchema( new Schema()
          .field("name",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("source_table")
    //4.etl

    val source_table = tableEnv.sqlQuery(
      """
        |select name,temperature from source_table
      """.stripMargin)

    //5.定义输出output
    val output = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\output.txt"
    //6.定义输出表
    tableEnv.connect(new FileSystem().path(output))
      .withFormat(new Csv)
      .withSchema(new Schema()
          .field("name",DataTypes.STRING())
          .field("temperture",DataTypes.DOUBLE())
      )
      .createTemporaryTable("output_table")

    //7.从输入表导数到输出表
    source_table.insertInto("output_table")

//    source_table.select()

    env.execute("file -> table -> table -file")

  }
}
