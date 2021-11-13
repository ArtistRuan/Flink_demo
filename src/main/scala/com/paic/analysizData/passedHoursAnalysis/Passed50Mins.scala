package com.paic.analysizData.passedHoursAnalysis

import java.lang
import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *
  * @program: FlinkEngine
  * @description:
  *              每5分钟统计过去50分钟内的用户的采购量
  *              select count(1),window(50,5)from table group by userId
  * @author: ruanshikao
  * @create: 2021-11-13 17:44
  *
  **/

object Passed50Mins {
  def main(args: Array[String]): Unit = {
    //初始化流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文本获取数据
    val sourcePath: URL = getClass().getResource("/UserBehavior.csv")
    val sourceStream: DataStream[String] = env.readTextFile(sourcePath.getPath)
    //将数据map成样例类
    val dataStream: DataStream[UserBehaviorPassed50Mins] = sourceStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        UserBehaviorPassed50Mins(
          dataArray(0).trim().toLong,
          dataArray(1).trim().toLong,
          dataArray(2).trim().toLong,
          dataArray(3).trim().toString,
          dataArray(4).trim().toLong
        )
      }
    )
    //watermark with timestamp
        .assignAscendingTimestamps(_.timestamp * 1000L)

    //定义业务逻辑：每5分钟统计过去50分钟内的用户的采购量
    //1.过滤数据，保留购买的数据
    val resultStream= dataStream.filter(_.behavior == "buy")
      //按用户id分组
      .keyBy(_.userId)
      //为了每五分钟统计过去50分钟的数据情况，需要开窗统计，窗口大小是50分钟，步长是5分钟
      .timeWindow(Time.minutes(50), Time.minutes(5))
    //定义聚合函数，用于读取每条数据后累计购买数据量
      .aggregate(new Passed50MinCountAgg(),new Passed50MinsWindowResult())

    resultStream
//        .filter(_.passed50MinsbuyCount > 10)
      .print("data sources")

    env.execute("user buy items in every 50 minutes")

  }
}

//定义输入数据样例类
case class UserBehaviorPassed50Mins(userId:Long,itemId:Long,categoryId:Long,behavior:String,timestamp:Long)
//定义输出数据样例类
case class UserBuyCount(userId:Long,passed50MinsbuyCount:Long,timestamp:Long)
//实现AggregateFunction()
//public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
class Passed50MinCountAgg() extends AggregateFunction[UserBehaviorPassed50Mins,Long,Long]{
  //创建初始化累加器，并初始化原始值
  override def createAccumulator(): Long = 0L
  //处理一条数据，即累加器 + 1
  override def add(in: UserBehaviorPassed50Mins, acc: Long): Long = acc + 1
  //getResult获取累加器的结果，当前返回累加器即可
  override def getResult(acc: Long): Long = acc
  //
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//WindowFunction()，将聚合的数据发送出去
//public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {
class Passed50MinsWindowResult() extends WindowFunction[Long,UserBuyCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[UserBuyCount]): Unit = {
    out.collect(UserBuyCount(key,input.iterator.next(),window.getEnd))
  }
}