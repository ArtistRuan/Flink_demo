package com.paic.analysizData.passedHoursAnalysis
import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

//640533,3168550,4719814,pv,1511690399
//定义输出数据的样例类
case class UserBehavior(UserID:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //1.创建执行程序
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val sourcePath: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(sourcePath.getPath)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toInt,
          dataArray(3).trim,
          dataArray(4).trim.toLong)
      })
      //*1000  要看是不是秒   秒*1000
      .assignAscendingTimestamps(_.timestamp*1000L)

    //3.transform 处理数据
    val processedStream = dataStream
        .filter(_.behavior == "pv")
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate(new CountAgg(),new WindowResult())
        .keyBy(_.windowEnd)
        .process(new TopNHotItems(3))

    //4.sink控制台输出
    processedStream.print()

    env.execute("hot items job")
  }

}
//自定义预聚合函数
//public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
//扩展 ---自定义预聚合函数计算平均数
//public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp,acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1 ,acc._2+acc1._2)
}
//自定义窗口函数，输出ItemViewCount   第一个Long是预聚合最终输出的long  第三个long是ItemId
//参数1：输入类型，即CountAgg的输出类型  参数2：输出类型  参数3：keyBy的返回值键值对中value的类型  参数4： 窗口类型
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long],
                     out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}
//自定义的处理函数   第一个是windowEnd所以是long
//public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction
class TopNHotItems(topsize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{

  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",
      classOf[ItemViewCount]))
  }
  //每来一条数据如何处理
  override def processElement(i: ItemViewCount, context: KeyedProcessFunction
    [Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每条数据存入状态列表
    itemState.add(i)
    //注册一个定时器  +1 是延迟时间
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }
  //定时器触发时的逻辑
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]
    #OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中的数据取出，放入到一个list buffer中
    val allItems:ListBuffer[ItemViewCount] = new ListBuffer()
    //将所有state中的数据取出，放入到一个list buffer中
//    import scala.collection.JavaConversions._
    for (item <- itemState.get()){
      allItems += item
    }
    //按照Count大小排序  并取前N个   sortBy是升序
//    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topsize)
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topsize)

    //清空状态   如果不想要下面的格式可以out.collect(sortedItems.toString())输出
    //UserBehavior(960222,4545844,2892802,pv,1511679926)
    itemState.clear()
    out.collect(sortedItems.toString())

    //将排名结果格式化输出  -1是之前注册定时器+1
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    //输出每一个商品信息
    for (i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append("商品ID =").append(currentItem.itemId)
        .append("浏览量").append(currentItem.count)
        .append("\n")
    }
    result.append("=====================")
    //控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}
