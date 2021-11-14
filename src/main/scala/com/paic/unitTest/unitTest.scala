//package com.paic.unitTest
//
//import java.net.URL
//
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.WindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
//import scala.collection.mutable.ListBuffer
//
////定义输入样例类
//case class Passed60MinsLoginTopNIn(userId:Long,loginIp:String,loginStatus:String,timestamp:Long)
////定义输出样例类
//case class Passed60MinsLoginTopNOut(loginIp:String,loginCount:Long,timestamp:Long)
//
///**
//  *
//  * @program: FlinkEngine
//  * @description: 测试
//  * @author: ruanshikao
//  * @create: 2021-11-14 12:51
//  *
//  **/
//object unitTest {
//  def main(args: Array[String]): Unit = {
//    //流执行环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    //设置并行度
//    env.setParallelism(4)
//    //设置时间语义
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //获取数据流
//    val sourcePath: URL = getClass.getResource("/LoginLog.csv")
//    val sourceStream: DataStream[String] = env.readTextFile(sourcePath.getPath)
//    val dataStream: DataStream[Passed60MinsLoginTopNIn] = sourceStream.map(
//      data => {
//        val dataArray: Array[String] = data.split(",")
//        Passed60MinsLoginTopNIn(
//          dataArray(0).trim.toLong,
//          dataArray(1).trim,
//          dataArray(2).trim,
//          dataArray(3).trim.toLong
//        )
//      }
//    )
//      //设置watermark
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Passed60MinsLoginTopNIn](Time.seconds(1)) {
//      override def extractTimestamp(t: Passed60MinsLoginTopNIn): Long = t.timestamp * 1000L
//    })
//    val resultStream = dataStream.filter(_.loginStatus == "success")
//      .keyBy(_.loginIp)
//      .timeWindow(Time.hours(1),Time.minutes(5))
//      .aggregate(new Passed60MinsLoginTopNCountAgg(),new Passed60MinsLoginTopNWindowResult())
//      .keyBy(_.timestamp)
//      .process(new Passed60MinsLoginProcessFunction(3))
//
//    resultStream.print()
//
//    env.execute()
//  }
//}
//
////预聚合:public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
//class Passed60MinsLoginTopNCountAgg() extends AggregateFunction[Passed60MinsLoginTopNIn,Long,Long]{
//  override def createAccumulator(): Long = 0L
//  override def add(in: Passed60MinsLoginTopNIn, acc: Long): Long = acc + 1
//  override def getResult(acc: Long): Long = acc
//  override def merge(acc: Long, acc1: Long): Long = acc + acc1
//}
////处理预聚合:trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {
//class Passed60MinsLoginTopNWindowResult() extends WindowFunction[Long,Passed60MinsLoginTopNOut,String,TimeWindow]{
//  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[Passed60MinsLoginTopNOut]): Unit = {
//    out.collect(Passed60MinsLoginTopNOut(key,input.iterator.next(),window.getEnd))
//  }
//}
//// public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
//class Passed60MinsLoginProcessFunction(topN:Int) extends KeyedProcessFunction[Long,Passed60MinsLoginTopNOut,String]{
//  private var itemState:ListState[Passed60MinsLoginTopNOut] = _
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction
//    [Long, Passed60MinsLoginTopNOut, String]#OnTimerContext, out: Collector[String]): Unit = {
//    var lb:ListBuffer[Passed60MinsLoginTopNOut] = new ListBuffer()
//    import scala.collection.JavaConversions._
//    for (item <- itemState.get()){
//      lb += item
//    }
//    //排序
//    val sortLb: ListBuffer[Passed60MinsLoginTopNOut] = lb.sortBy(_.loginCount)(Ordering.Long.reverse).take(topN)
//    itemState.clear()
//    out.collect(sortLb.toString())
//  }
//
//  override def processElement(i: Passed60MinsLoginTopNOut, context: KeyedProcessFunction[Long, Passed60MinsLoginTopNOut, String]
//    #Context, collector: Collector[String]): Unit = {
//    itemState.add(i)
//    context.timerService().registerEventTimeTimer(i.timestamp + 1)
//  }
//
//  override def open(parameters: Configuration): Unit = {
//    itemState = getRuntimeContext.getListState(new ListStateDescriptor[Passed60MinsLoginTopNOut](
//      "Passed60MinsLoginTopNOut-ListState",
//      classOf[Passed60MinsLoginTopNOut]
//    ))
//  }
//}