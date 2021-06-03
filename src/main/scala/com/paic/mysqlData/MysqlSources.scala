package com.paic.mysqlData

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-23 17:52
  *
  **/
object MysqlSources {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputMysql: DataSet[Row] = jdbcRead(env)
    inputMysql.print()

  }
//利用有返回值的方式，可以用获取到的数据根据业务进一步操作
  def jdbcRead(env:ExecutionEnvironment)={
    val inputMysql:DataSet[Row] = env.createInput(
      JDBCInputFormat.buildJDBCInputFormat()
    //指定驱动名称
        .setDrivername("com.mysql.jdbc.Driver")
        //url
        .setDBUrl("jdbc:mysql://localhost:3306/test")
        .setUsername("root")
        .setPassword("123456")
        .setQuery("select id,username,password from web_test.administrator")
        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO))
        .finish()
    )
    inputMysql
  }
}


