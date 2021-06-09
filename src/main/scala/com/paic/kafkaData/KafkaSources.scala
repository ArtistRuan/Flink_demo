package com.paic.kafkaData

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.util.Random

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-23 12:43
  *
  **/
object KafkaSources {
  def main(args: Array[String]): Unit = {

    fromDefaultKafka
//    fromDefinedSources
  }

  //add sources from kafka
  def fromDefaultKafka(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.174.100:9092")
    properties.setProperty("group.id","consumer-group")

    val streamKafka: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(),properties))

    streamKafka.print()

    env.execute("Data From Kafka Stream")
  }

  //add defined sources
  def fromDefinedSources(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val streamDefinedData: DataStream[SensorReading] = env.addSource(new MySensorSource)

    streamDefinedData.print()

    env.execute("Add Defined Sources To Flink")
  }
}

//自定义source function
class MySensorSource extends SourceFunction[SensorReading]{
  //定义一个标识位flag：用于表示数据源是否正常运行发出数据
  var running: Boolean = true
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //模拟动态温度传感器，定义一个随机数
    val rand = new Random()

    //随机生成一组（10个）传感器初始温度:(id,temp)
    var curTemp = 1.to(10).map( x => ("sensor_" + x , rand.nextDouble() * 100))

    //定义一个无限循环，用于产生数据，除非被cancel掉
    while (running){
      //在上次的基础上微调，更新温度值
      curTemp = curTemp.map(
        data => (data._1,data._2 + rand.nextGaussian())
      )

      val dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      //获取当前时间戳，加入到数据中，调用ctx发出数据
//      val curTime = System.currentTimeMillis()

      val dateStr: String = dateformat.format(System.currentTimeMillis)

      curTemp.foreach(
        data => ctx.collect(SensorReading(data._1,dateStr,data._2))
      )
      //间隔1000s
      Thread.sleep(1000)
    }
  }

  //当标识位为false的时候，执行cancel
  override def cancel(): Unit = running = false
}

//定义温度器样例类
case class SensorReading(sensorName:String,recordTime:String,temperature:Double)
case class SensorTem(id:String,timestamp:Long,temperature:Double)
//温度器名称样例类
case class SensorName(sensorName:String)
