package com.paic.stateManager

import java.util

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-31 11:26
  *
  **/
object ListState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


  }
  def listState(env:StreamExecutionEnvironment): Unit ={
    val socketSourceStream: DataStream[String] = env.socketTextStream("localhost",7777)
    socketSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    env.execute()
  }

}

class MyListState extends RichMapFunction[Person,String]{
  lazy val listState:ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState",classOf[Int]))

  override def map(value: Person): String = {

    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)
    value.motherland
  }
}
