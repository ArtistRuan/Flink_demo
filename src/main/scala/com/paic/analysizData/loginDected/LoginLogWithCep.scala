package com.paic.analysizData.loginDected

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入样例类
case class loginInfo(userId:Long,loginIp:String,loginStatus:String,timestamp:Long)
//输出样例类
case class loginWarning(userId:Long,firstFail:String,secondFail:String,warningMsg:String)


/**
  *
  * @program: FlinkEngine
  * @description: CEP Demo
  * @author: ruanshikao
  * @create: 2021-09-08 23:52
  *
  **/
object LoginLogWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source: URL = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(source.getPath)
//    val inputStream = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\LoginLog.csv")
      .map(data =>{
        val arr = data.split(",")
        loginInfo(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[loginInfo](Time.seconds(2)) {
        override def extractTimestamp(t: loginInfo): Long = t.timestamp * 1000L
      })

    //定义CEP模式

    val pattern = Pattern
      .begin[loginInfo]("firstFail").where(_.loginStatus == "fail")
      .next("secondFail").where(_.loginStatus == "fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(inputStream.keyBy(_.userId),pattern)


    val loginFailPattern = patternStream.select(new loginPattern())

    loginFailPattern.print()

    env.execute("CEP job")

  }
}

class loginPattern extends PatternSelectFunction[loginInfo,loginWarning]{
  override def select(pattern: util.Map[String, util.List[loginInfo]]): loginWarning = {
    //获取时间序列
    val firstFail = pattern.get("firstFail").get(0)
    val secondFail = pattern.get("secondFail").iterator().next()

    loginWarning(firstFail.userId,firstFail.timestamp.toString,secondFail.timestamp.toString,"loginFail in 2 seconds")
  }
}