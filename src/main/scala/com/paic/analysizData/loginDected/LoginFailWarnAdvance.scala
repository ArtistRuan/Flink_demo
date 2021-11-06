package com.paic.analysizData.loginDected

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.util.Collector

/**
  *
  * @program: FlinkEngine
  * @description: 如果2秒内有多次失败又有成功，那就避开了触发器告警，故此版本针对这个做优化
  * @author: ruanshikao
  * @create: 2021-09-09 23:44
  *
  **/

//输入样例类
//case class loginInfo(user_id:Long, ip:String, eventType:String, timestamp:Long)
//输出样例类
case class loginFailWarning(userId:Long, lastFailTime:Long, thisFailTime:Long, msg:String)

object LoginFailWarnAdvance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(4)

    val source = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(source.getPath)
      .map(data => {
        val arr: Array[String] = data.split(",")
        loginInfo(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[loginInfo](Time.seconds(2)) {
        override def extractTimestamp(t: loginInfo): Long = t.timestamp * 1000L
      })

    val loginFailStream = inputStream
      .keyBy(_.userId)
      .process( new LoginFailAdvanceResult(2) )

    loginFailStream.print()
    env.execute("login fail advance")
  }
}

class LoginFailAdvanceResult(FailTime: Int) extends KeyedProcessFunction[Long,loginInfo,loginFailWarning]{
  //定义状态，因会有多次多个状态，用ListState比较好，方便扩展
  lazy val loginFailListState: ListState[loginInfo] = getRuntimeContext.getListState(new ListStateDescriptor[loginInfo]("login-list",classOf[loginInfo]))

  override def processElement(value: loginInfo, context: KeyedProcessFunction[Long, loginInfo, loginFailWarning]#Context, collector: Collector[loginFailWarning]): Unit = {
    //每来一条数据，都会调用的processElement方法
    //对进来的数据时间进行判断
    if(value.loginStatus == "fail"){
      val loginEventIter = loginFailListState.get().iterator()
      if(loginEventIter.hasNext){
        //如果之前已经有了状态，则判断2次的时间戳相差多少
        val firstFail = loginEventIter.next()
        if(value.timestamp < firstFail.timestamp + 2000L){
          collector.collect(loginFailWarning(firstFail.userId,firstFail.timestamp,value.timestamp,"login fail in 2s for 2 times"))
        }
        //不管报警不报警，当前都已经处理完毕，将状态更新为最近一次的登录失败事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        //否则将本次到来的状态添加到List即可
        loginFailListState.add(value)
      }
      //进一步判断
    } else {
      //清空状态
      loginFailListState.clear()
    }
  }
}