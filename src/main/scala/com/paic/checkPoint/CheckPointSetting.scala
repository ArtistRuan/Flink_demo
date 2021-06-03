package com.paic.checkPoint

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-03 16:20
  *
  **/
object CheckPointSetting {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(1000L,CheckpointingMode.EXACTLY_ONCE)

    //方法2
    env.enableCheckpointing(1000L) // 1秒钟一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L) //1分钟超时

    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) //最大并发的checkpoint数量
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) //2个checkpoint之间的最小间隔时间，这与上面max会有冲突

    env.getCheckpointConfig.setFailOnCheckpointingErrors(true) //是否容忍有checkpoint失败，如false则checkpoint失败引发整个程序失败
    //新版本用 env.getCheckpointConfig.setTolerableCheckpointFailureNumber(number:Int)

    //重启策略，2种常用
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,60000L)) //一共重启3次，每次间隔一分钟
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5,TimeUnit.MINUTES),Time.of(5,TimeUnit.MINUTES)))


  }
}
