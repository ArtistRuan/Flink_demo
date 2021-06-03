package com.paic

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-26 16:58
  *
  **/
object DataPipeline {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //可以正常执行，但如果传入的数据位数不够会报错索引越界，如果第二个是字符串报错类型不匹配
    fromKafkaToMysql(env)

  }

  def fromKafkaToMysql(env:StreamExecutionEnvironment): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.100:9092")
    properties.setProperty("group.id","consumer-group")

    //获取从kafka过来的数据
    // bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic epmsgoods
    val sourceStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("epmsgoods",new SimpleStringSchema(),properties))
    //定义业务逻辑
    val dataSource: DataStream[EpmsGoods] = sourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        //sku:String,price:Double,upTime:String
        EpmsGoods(arr(0),arr(1).toDouble,arr(2))
      }
    )
      //这里的逻辑返回类型不匹配，未解决
//    val resultStream: DataStream[Any] = sourceStream.map(
//      data => {
//        val array: Array[String] = data.split(",")
//        if(array.length == 3){
//          EpmsGoods(array(0),array(1).toDouble,array(2))
//        }
//      }
//    )

    dataSource.addSink(new EpmsGoodsJDBCMysql())

    dataSource.print("kafkaSources")

    print("开始执行")

    env.execute("StreamData From Kafka To Mysql")
  }
}

class EpmsGoodsJDBCMysql() extends RichSinkFunction[EpmsGoods]{
  //定义全局连接、预编译语句
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into epmsGoods (sku,price,upTime) values (?,?,?)")
    updateStmt = conn.prepareStatement("update epmsGoods set price = ? where sku = ?")

  }

  override def invoke(value: EpmsGoods): Unit = {
    //实际数据操作，先更新，如果没有则插入
    updateStmt.setDouble(1,value.price)
    updateStmt.setString(2,value.sku)
    //提交执行
    updateStmt.execute()
    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.sku)
      insertStmt.setDouble(2,value.price)
      insertStmt.setString(3,value.upTime)
      //提交执行
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}