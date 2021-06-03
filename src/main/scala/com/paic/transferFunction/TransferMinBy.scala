package com.paic.transferFunction

import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-23 20:41
  *
  **/
object TransferMinBy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    readSourceFromText(env)



  }

  def readSourceFromText(env:StreamExecutionEnvironment){
    val inputPath = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    //输出每个人分数
    val aggStream: DataStream[Person] = dataStream
      .keyBy("name")
//      .max("score")
        .maxBy("score")

    val resultStream: DataStream[Person] = dataStream
        .keyBy("name")
        .reduce( (curState,newData) =>
        Person(curState.name,curState.score.max(newData.score),curState.motherland)
        )

    //多流-分流 split后传入lambada表达式
    val splitStream = dataStream.split( data => {
      if(data.score > 79 ) Seq("goodMark") else Seq("badMark")
    })
    val goodMarkStream = splitStream.select("goodMark")
    val badMarkStream = splitStream.select("badMark")
    val allMarkStream = splitStream.select("goodMark","badMark")

//    resultStream.print()

    goodMarkStream.print("goodMark")
    badMarkStream.print("badMark")
    allMarkStream.print("all")

    env.execute("KeyBy")
  }

}


case class Person(name:String,score:Int,motherland:String)
