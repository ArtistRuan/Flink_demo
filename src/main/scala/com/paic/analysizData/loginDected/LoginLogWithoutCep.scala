package com.paic.analysizData.loginDected

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  *
  * @program: FlinkEngine
  * @description: 两次登录失败告警，process处理
  * @author: ruanshikao
  * @create: 2021-09-09 01:01
  *
  **/
//输入样例类
//case class loginInfo(user_id:Long, ip:String, eventType:String, timestamp:Long)
//输出样例类
case class loginFailWarning(userId:Long, lastFailTime:Long, thisFailTime:Long, msg:String)

object LoginLogWithoutCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(source.getPath)
      .map(data => {
        val arr = data.split(",")
        loginInfo(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[loginInfo](Time.seconds(2)) {
      override def extractTimestamp(t: loginInfo): Long = t.timestamp * 1000L
    })

    val loginFailWarningStream = inputStream
      .keyBy(_.userId)
      .process(new LoginFailWarningResult(2))

    loginFailWarningStream.print()

    env.execute("process status coding")

  }
}


class LoginFailWarningResult(failTime:Int) extends KeyedProcessFunction[Long,loginInfo,loginFailWarning]{
  //定义状态，保存当前所有的登录失败时间，保存定时器的时间戳
  lazy val loginFailListState: ListState[loginInfo] = getRuntimeContext.getListState(new ListStateDescriptor[loginInfo]("loginFail-list",classOf[loginInfo]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

  override def processElement(value: loginInfo, context: KeyedProcessFunction[Long, loginInfo, loginFailWarning]#Context, collector: Collector[loginFailWarning]): Unit = {
    if(value.loginStatus == "fail"){
      loginFailListState.add(value)
      if(timerTsState.value() == 0){
        val ts = value.timestamp * 1000L + 2000L
        context.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      //如果是成功，清空状态和定时器，从头可计算
      context.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()

    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, loginInfo, loginFailWarning]#OnTimerContext, out: Collector[loginFailWarning]): Unit = {
    val allLoginFailList: ListBuffer[loginInfo] = new ListBuffer[loginInfo]()
    val iter = loginFailListState.get().iterator()
    while (iter.hasNext){
      allLoginFailList += iter.next()
    }
    //判断登录告警时间的个数，超过上限，输出告警
    if(allLoginFailList.length >= failTime){
      out.collect(loginFailWarning(
        allLoginFailList.head.userId,
        allLoginFailList.head.timestamp,
        allLoginFailList.last.timestamp,
        "login fail " + failTime + " in 2 seconds"
      ))
    }
    //完成计算后，清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}