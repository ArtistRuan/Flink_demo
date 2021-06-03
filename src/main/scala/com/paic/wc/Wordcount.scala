package com.paic.wc

import org.apache.flink.api.scala._

object Wordcount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val source = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt"
    val sourceDS: DataSet[String] = env.readTextFile(source)

    val result = sourceDS.flatMap(_.split(" "))
      .filter(_.nonEmpty).map(x => (x,1))
      .groupBy(0)
      .sum(1)

    result.print()

  }
}
