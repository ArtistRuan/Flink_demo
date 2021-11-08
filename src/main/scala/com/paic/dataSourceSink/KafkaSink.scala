package com.paic.dataSourceSink

import java.net.URL
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-11-09 00:56
  *
  **/
object KafkaSink {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(4)
    //从文件获取数据
    val sourcePath: URL = getClass.getResource("/source.txt")
    val sourceStream: DataStream[String] = env.readTextFile(sourcePath.getPath)
    //定义kafka连接信息
    //sink，因为将数据sink到kafka，所以创建生产者
    sourceStream.addSink(
      new FlinkKafkaProducer010[String](
        "192.168.174.200:9092",
        "topicSink",
        new SimpleStringSchema())
    )
    //启动任务
    env.execute("kafka producer sink")

  }
}
