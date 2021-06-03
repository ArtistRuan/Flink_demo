package com.paic.stateManager

import com.paic.kafkaData.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-31 16:37
  *
  **/
object TemperatureWarning {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    temperateWaring(env)
  }
  //需求：对10秒内温度连续上升进行报警
  def temperateWaring(env:StreamExecutionEnvironment): Unit ={
   val socketSourceStream: DataStream[String] = env.socketTextStream("localhost",7777)
    val sourceStream: DataStream[SensorReading] = socketSourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1), arr(2).toDouble)
      }
    )
    val warningStream = sourceStream
      .keyBy(_.sensorName)
      .process( new TempeIncreWaning(10000L))

    warningStream.print()

    env.execute()
  }
}

class TempeIncreWaning(interval:Long) extends KeyedProcessFunction[String,SensorReading,String]{
  //定义状态：保留上一个温度值进行比较，保存注册定时器的时间戳用于删除
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))
  lazy val lastTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-timeTs",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //取出当前状态
    val lastTmp = lastTempState.value()
    val lastTs = lastTsState.value()

    if(value.temperature > lastTmp && lastTs == 0){
      //如果温度上升并且没有定时器，那么注册当前时间戳10秒后的定时器
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      //状态保存，把当前时间戳更新
      lastTsState.update(ts)
    } else if(value.temperature < lastTmp){
      //如果温度在10秒内下降，则删除定时器
      ctx.timerService().deleteProcessingTimeTimer(lastTs)
      lastTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "温度连续" + interval/1000 + "秒连续上升")
    lastTsState.clear()
  }
}