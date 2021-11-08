package com.paic.dataSourceSink

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-11-06 18:32
  *
  **/
object FileSource {
  def main(args: Array[String]): Unit = {
    //初始化流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置全局并行度
    env.setParallelism(4)
    //获取数据源
    val source_path: URL = getClass.getResource("/source.txt")
    val sourceStream: DataStream[String] = env.readTextFile(source_path.getPath)

    //执行业务操作
    sourceStream.print("数据")

    //启动任务
    env.execute("source from text file")

  }
}
