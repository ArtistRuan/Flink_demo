package com.paic.stateManager

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-31 14:45
  *
  **/
object StateMangedDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    definedStateManage(env)
  }
  def definedStateManage(env:StreamExecutionEnvironment): Unit ={
    val socketSourceStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val sourceStream = socketSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    val alertStream = sourceStream.keyBy(_.name)
      .flatMap( new HugeChangeAlert(10) )

    alertStream.print()

    env.execute()
  }
}

//实现自定义richflatmapfunction
class HugeChangeAlert(threshold: Int) extends RichFlatMapFunction[Person,(String,Int,Int)]{
  //定义记录上一次状态
  lazy val valueState:ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("last-state",classOf[Int]))
  //定义一个flag用于初始值不要做对比风控告警(也可用初始状态判断方式)
  var flag:Boolean = true

  override def flatMap(value: Person, out: Collector[(String, Int, Int)]): Unit = {

    if(flag){
      flag = false
    } else {
      //将上次的状态拿出来
      val lastState = valueState.value()
      //与最新的状态值作比较
      val diff = (value.score - lastState).abs
      if(diff > threshold){
        out.collect((value.name,lastState,value.score))
      }
    }
    //更新当前状态值
    valueState.update(value.score)
  }
}
