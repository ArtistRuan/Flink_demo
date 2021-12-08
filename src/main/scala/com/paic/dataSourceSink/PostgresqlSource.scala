package com.paic.dataSourceSink

/**
  * 这份pg的代码可以用于从pg库中读取数据
  */

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

//定义输出样例类
case class Pg_Student(name:String,age:Int,address:String)

object PostgresqlSource{
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(4)
    //定义输入流
    val sourceStream: DataStream[Pg_Student] = env.addSource(new PgSource())
    sourceStream.print("输入的数据为：")

    //启动任务
    env.execute(this.getClass.getSimpleName)
  }
}

class PgSource() extends RichSourceFunction[Pg_Student]{
//  //定义驱动
//  val driverClass = "org.postgresql.Driver"
//  Class.forName(driverClass)
  //定义连接
  var conn: Connection = _
  var pstmt: PreparedStatement = _
  var running = true
  override def open(parameters: Configuration): Unit = {
    //设置连接
    conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres","Paic1234")
    pstmt = conn.prepareStatement("select * from student_basic_info")
  }

  override def close(): Unit = {
    pstmt.close()
    conn.close()
  }

  override def run(sourceContext: SourceFunction.SourceContext[Pg_Student]): Unit = {
    //获取数据，到样例类
    val res = pstmt.executeQuery()
    while(res.next()){
      sourceContext.collect(Pg_Student(res.getString("name"),res.getInt("age"),res.getString("address")))
    }
  }

  override def cancel(): Unit = running = false
}