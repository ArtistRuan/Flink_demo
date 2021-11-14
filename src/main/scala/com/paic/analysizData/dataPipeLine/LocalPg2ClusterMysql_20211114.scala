package com.paic.analysizData.dataPipeLine

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description:
  *              本地pg数据通过数据管道推送到远程mysql数据库
                 select count(1) from almart_all;  --521.372s  300000000 rows
  * @author: ruanshikao
  * @create: 2021-11-14 18:28
  *
  **/
object LocalPg2ClusterMysql_20211114 {
  def main(args: Array[String]): Unit = {
    //流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(4)
    //设置重启
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,60000L))

  }
}
