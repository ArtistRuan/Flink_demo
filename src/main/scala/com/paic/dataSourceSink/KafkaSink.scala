package com.paic.dataSourceSink

import java.util.Properties

import com.paic.transferFunction.Person
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-24 23:08
  *
  **/
object KafkaSink {  def main(args: Array[String]): Unit = {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  fromTextSinkKafka(env)

  }
//从文本到kafka
  def fromTextSinkKafka(env:StreamExecutionEnvironment): Unit ={
    val inPath = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt"
    val dataSource: DataStream[String] = env.readTextFile(inPath)

    val streamSource = dataSource.map(
      data => {
        val arr = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2)).toString
      }
    )

    streamSource.addSink(
      new FlinkKafkaProducer010[String]("192.168.174.200:9092","topicSink",new SimpleStringSchema())
    )

  }
//从kafka到kafka（数据管道：flink在中间作为处理引擎，数据从一端流向一端）
  def fromKafkaSinkKafka(env: StreamExecutionEnvironment): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.server","192.168.174.200:9092")
    properties.setProperty("group.id","consumer-group")

    //从kafka获取数据
    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(),properties))

    //可以加入对数据的处理转换

    //将处理过的数据sink到kafka
    kafkaStream.addSink(new FlinkKafkaProducer010[String]("192.168.174.200:9092","kafkaSink",new SimpleStringSchema()))
  }

}
