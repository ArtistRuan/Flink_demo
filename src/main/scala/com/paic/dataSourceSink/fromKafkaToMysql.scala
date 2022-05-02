package com.paic.dataSourceSink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  *
  * @program: FlinkEngine
  * @description: 将kafka中的数据消费，处理完毕后sink到mysql（依然是30条数据每秒）
  * @author: ruanshikao
  * @create: 2022-05-01 19:22
  *
  **/
case class kafkaSource(str1:String,str2:String,str3:String,str4:String,log:Long)
object fromKafkaToMysql {
  def main(args: Array[String]): Unit = {
    // 获取流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","node1:9092")
    properties.setProperty("group.id","consumer-group")
//    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("auto.offset.reset", "latest")

    val start_time = System.currentTimeMillis()
    println("开始时间是：" + start_time)

    //从kafka获取数据
    val kafkaStream: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer010[String](
        "my-flink-kafka",
        new SimpleStringSchema(),
        properties)
      )

    kafkaStream.print()

    val source_stream: DataStream[kafkaSource] = kafkaStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        kafkaSource(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
      }
    )
    // 将数据打印到控制台
    source_stream.print("数据是:")
    source_stream.addSink(new MyJDBCMysqlSinkfromKafka())

    env.execute("from kafka to mysql")

    val end_time: Long = System.currentTimeMillis()
    val execute_time = (end_time - start_time) / 1000
    println("执行时长：" + execute_time)
  }
}

class MyJDBCMysqlSinkfromKafka() extends RichSinkFunction[kafkaSource]{
  println("sink to mysql db")
  // 初始化一个连接
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  override def invoke(value: kafkaSource, context: SinkFunction.Context[_]): Unit = {
    insertStmt.setString(1,value.str1)
    insertStmt.setString(2,value.str2)
    insertStmt.setString(3,value.str3)
    insertStmt.setString(4,value.str4)
    insertStmt.setLong(5,value.log)
    println("字段1：" + value.str1)
    println("字段2：" + value.str2)
    println("字段3：" + value.str3)
    println("字段4：" + value.str4)
    println("字段5：" + value.log)
    insertStmt.execute()
  }

  override def open(parameters: Configuration): Unit = {
    // 定义驱动
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink","root","123456")
    // 定义预编译语句
    insertStmt = conn.prepareStatement("insert into flink.webview_from_kafka(web_id,city_id,user_id,action_name,action_time) values (?,?,?,?,?)")
  }

  override def close(): Unit = {
    conn.close()
    insertStmt.close()
  }
}
