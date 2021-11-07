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
  * @description: 将数据sink到mysql，发现bug就是这个代码无法实现upsert模式，目前的数据是会有重复的，即：
  * 源数据：
  *               ALEX 90 CN
  *               ALEX 99 BJ
  *               MIKE 80 US
  *               MIKE 77 UK
  *               JACK 60 CA
  *               JACK 60 JP
  * 结果数据：
  *               ALEX 99
  *               ALEX 99
  *               MIKE 77
  *               MIKE 77
  *               JACK 60
  *               JACK 60
  * @author: ruanshikao
  * @create: 2021-05-26 00:11
  *
  **/
object MysqlSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
    dataSource.print()
    dataSource.addSink(new MyJdbdSinkFunc())

    env.execute("Data Sink To Mysql")

  }
}

class MyJdbdSinkFunc() extends RichSinkFunction[Person]{
  //定义连接、预编译语句
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
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