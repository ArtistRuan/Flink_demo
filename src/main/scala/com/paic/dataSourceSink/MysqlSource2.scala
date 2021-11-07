package com.paic.dataSourceSink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: 从数据库中读取数据后做逻辑操作
  * @author: ruanshikao
  * @create: 2021-11-06 18:33
  *
  **/
//定义输出样例类
case class Student_info(name:String,age:Int,birthday:String)

object MysqlSource2 {
  def main(args: Array[String]): Unit = {
    // 获取流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //总体采用不同的并行度
//    env.setParallelism(1)
    env.setParallelism(4)

    //定义addsource
    val sourceStream: DataStream[Student_info] = env.addSource(new mysqlSource())
    sourceStream.print("addsource()数据")

    //执行
//    env.execute("flink mysql source")
    env.execute(this.getClass.getSimpleName)
  }
}

class mysqlSource() extends RichSourceFunction[Student_info]{
  //定义连接
  var conn: Connection = _
  var pstmt: PreparedStatement = _
  var running = true
  override def open(parameters: Configuration): Unit = {
    //获取连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/student","root","123456")
    pstmt = conn.prepareStatement("select * from student")
  }

  override def close(): Unit = {
    pstmt.close()
    conn.close()
  }

  override def run(sourceContext: SourceFunction.SourceContext[Student_info]): Unit = {
    val res = pstmt.executeQuery()
    while(res.next()){
      val student = Student_info(res.getString("name"),res.getInt("age"),res.getString("birthday"))
      sourceContext.collect(student)
    }
  }

  override def cancel(): Unit = running = false
}