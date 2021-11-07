package com.paic.dataSourceSink

import java.net.URL
import java.sql.{Connection, DriverManager, PreparedStatement}

import com.paic.transferFunction.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: 将数据sink到mysql，采用upsert模式
  * @author: ruanshikao
  * @create: 2021-05-26 00:11
  *
  **/
object MysqlSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    dataSinkMysql(env)


  }

  def dataSinkMysql(env:StreamExecutionEnvironment): Unit ={
    val sourcePath: URL = getClass.getResource("/source.txt")
    val textDataSource: DataStream[String] = env.readTextFile(sourcePath.getPath)

    val dataSource = textDataSource.map(
      data => {
        val arr: Array[String] = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    //print
    dataSource.print("数据集")
    dataSource.addSink(new MyJdbdSinkFunc()).setParallelism(4)

    env.execute("Data Sink To Mysql")

  }
}

class MyJdbdSinkFunc() extends RichSinkFunction[Person]{
  //定义连接、预编译语句
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
    //定义驱动
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    //定义预编译语句
    insertStmt = conn.prepareStatement("insert into student (name,score) values (?,?)")
    updateStmt = conn.prepareStatement("update student set score = ? where name = ?")
  }

  override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {
    //先执行更新操作，查到就更新
    updateStmt.setInt(1,value.score)
    updateStmt.setString(2,value.name)
//    updateStmt.executeUpdate()
    updateStmt.execute()
    //如果更新没有查到数据，那么就插入
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.name)
      insertStmt.setInt(2,value.score)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}