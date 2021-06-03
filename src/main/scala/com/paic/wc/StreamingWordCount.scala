package com.paic.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从外部命令中提取参数，作为socket主机名和端口名
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    val source: DataStream[String] = env.socketTextStream(host,port)

    val sourceStream = source.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map(x => (x,1))
      .keyBy(0)
      .sum(1)

    sourceStream.print().setParallelism(1)

    //启动任务
    env.execute()
  }
}
