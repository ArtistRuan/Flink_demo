package com.paic.stateManager

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-31 10:57
  *
  **/
object StateManager {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


  }

  def stateManager(env:StreamExecutionEnvironment): Unit ={
    val socketSourceStream: DataStream[String] = env.socketTextStream("localhost",7777)

    val sourceStram: DataStream[Person] = socketSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    env.execute()
  }
}
//keyed state必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[Person,Int]{

  var valueState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("valuestate",classOf[Int]))

  }

  override def map(value: Person):Int = {
    val readValue: Int = valueState.value()
    val updateValue: Unit = valueState.update(value.score)
    value.score

  }
}