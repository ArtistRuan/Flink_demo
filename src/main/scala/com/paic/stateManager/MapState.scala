package com.paic.stateManager

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-31 11:55
  *
  **/
object MapState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

  }
  def mapState(env:StreamExecutionEnvironment): Unit ={
    val socketSourceState: DataStream[String] = env.socketTextStream("localhost",7777)
    val sourceStream = socketSourceState.map(
      data => {
        val arr: Array[String] = data.split(",")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    env.execute()
  }
}

class MyMapState extends RichMapFunction[Person,String]{
  lazy val mapState:MapState[String,Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Int]("mapState",classOf[String],classOf[Int]))

  override def map(value: Person): String = {
    mapState.contains("alex")
    mapState.get("alex")
    mapState.put("alex",100)

    value.score.toString
  }
}
