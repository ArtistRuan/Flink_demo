package com.paic.checkPoint

import com.paic.kafkaData.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-03 17:25
  *
  **/
object SavePointSetting {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


  }

  def savePointSetting(env:StreamExecutionEnvironment): Unit ={
    val socketDataStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val sourceStream = socketDataStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0),arr(1),arr(2).toDouble)
      }
    )
    //保存点uid(record:String) 如果不手动提供record，flink会随机提供一个，但不利于根据需求做savePoint恢复
      .uid("202106031731")

  }
}
