package com.paic.dataSourceSink

import java.net.URL
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-11-06 18:33
  *
  **/

//输入样例类
case class Person_info(name:String,age:Int,address:String)

object PostgresqlSink {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(4)
    env.setParallelism(1)

    //获取输入数据文件名
    val source_path: URL = getClass.getResource("/source.txt")
    //获取输入数据流
    val sourceStream = env.readTextFile(source_path.getPath)
    val inputStream = sourceStream.map(
      data => {
        val data_arr = data.split(" ")
        Person_info(data_arr(0),data_arr(1).toInt,data_arr(2))
      })
    //打印输入数据
    inputStream.print()

    inputStream.addSink(new PgjdbcSink())

    env.execute("PG sink")
  }
}

//定义输出sink
class PgjdbcSink() extends RichSinkFunction[Person_info]{
  //定义连接串
  var conn:Connection =_
  var updateStatement: PreparedStatement = _
  var insertStatement: PreparedStatement = _

  //JDBC连接信息

  val USERNAME = "postgres"

  val PASSWORD = "Paic1234"

  val driverClass = "org.postgresql.Driver"

  val URL = "jdbc:postgresql://localhost:5432/postgres"

  //加载jdbc的驱动
  Class.forName(driverClass)

  //定义连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection(URL,USERNAME,PASSWORD)
    //    conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres","Paic1234")
    updateStatement = conn.prepareStatement("update student_basic_info set address = ? where name = ?")
    insertStatement = conn.prepareStatement("insert into student_basic_info (name,age,address) values (?,?,?)")

  }
  //定义操作
  override def invoke(value: Person_info, context: SinkFunction.Context[_]): Unit = {
    updateStatement.setString(1,value.address)
    updateStatement.setString(2,value.name)
    updateStatement.execute()
    if(updateStatement.getUpdateCount == 0){
      insertStatement.setString(1,value.name)
      insertStatement.setInt(2,value.age)
      insertStatement.setString(3,value.address)
      insertStatement.execute()
    }
  }
  //关闭连接
  override def close(): Unit = {
    updateStatement.close()
    insertStatement.close()
    conn.close()
  }
}