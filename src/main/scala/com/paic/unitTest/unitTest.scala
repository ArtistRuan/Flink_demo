package com.paic.unitTest

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  *
  * @program: FlinkEngine
  * @description: 测试
  * @author: ruanshikao
  * @create: 2021-11-14 12:51
  *
  **/
object unitTest {
  def main(args: Array[String]): Unit = {
    //流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(4)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取数据流

  }
}