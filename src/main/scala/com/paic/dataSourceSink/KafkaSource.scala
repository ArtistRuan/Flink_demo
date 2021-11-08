package com.paic.dataSourceSink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-11-06 18:32
  *
  **/
object KafkaSource {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(4)
    //设置kafka连接信息
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","192.168.174.200:9092")
    prop.setProperty("group.id","consumer-group")
    //连接kafka并获取数据
    val kafkaSource: DataStream[String] = env.
      addSource(new FlinkKafkaConsumer010[String](
        "sensor",
        new SimpleStringSchema(),
        prop
      ))

    //执行数据逻辑
    kafkaSource.print("kafka 数据：")

    //启动任务
    env.execute("kafka source")
  }
}
