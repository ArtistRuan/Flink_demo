package com.paic.dataSourceSink

import com.paic.transferFunction.Person
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-24 22:12
  *
  **/
object FileSink {
  def main(args: Array[String]): Unit = {

    //如果不是流环境，不可以往kafka等写入数据，只有文件
//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //创建流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    dataFromFile(env)
  }

  def dataFromFile(env:StreamExecutionEnvironment){
//    val inputPath = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt"
    //使用getClass.getSource
    val sourcePath = getClass.getResource("/source.txt")
//    val dataSource: DataStream[String] = env.readTextFile(inputPath)
    val dataSource: DataStream[String] = env.readTextFile(sourcePath.getPath)

    val dataStream = dataSource.map(
      data => {
        val arr: Array[String] = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )
    dataStream.print()
    //问题：如果文件存在，如何删除先（否则报错）?
    dataStream.writeAsCsv("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\out.txt",FileSystem.WriteMode.OVERWRITE)
    dataStream.writeAsText("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\text.txt",FileSystem.WriteMode.OVERWRITE)
    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\outWithSink2.txt"),
      new SimpleStringEncoder[Person]()
      ).build()
    )
    env.execute("File Sink")
  }
}
