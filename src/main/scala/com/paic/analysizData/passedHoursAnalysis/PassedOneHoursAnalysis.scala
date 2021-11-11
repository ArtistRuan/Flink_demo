package com.paic.analysizData.passedHoursAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  *
  * @program: FlinkEngine
  * @description:
  *              用于统计过去商品点击数
  *              每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品（flink+kafka）
  * @author: ruanshikao
  * @create: 2021-11-11 23:09
  *
  **/

/**定义输入样例类
543462,1715,1464116,pv,1511658000
加密后的用户ID，加密后的商品ID，加密后的商品所属类别ID，用户行为类型（包括pv/buy/fav），行为发生的时间戳，单位秒
*/
case class UserBehavior(UserID:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

/**
  * 定义输出样例类（主键商品ID，窗口，点击量）
  */
case class ItemViewCount(itemId:Long,windowEnd:Long,clickCount:Long)

object PassedOneHoursAnalysis {
  def main(args: Array[String]): Unit = {
    //获取流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(2)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取输入数据，方法1：从文本
//    val sourcePath: URL = getClass.getResource("/UserBehavior.csv")
//    val sourceStream: DataStream[String] = env.readTextFile(sourcePath.getPath)
    val sourceStream: DataStream[String] = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = sourceStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val processStream = dataStream.filter(_.behavior == "pv")
      //根据商品ID进行keyby分组，然后开窗统计窗口大小为1小时，步长为5分钟的窗口
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1),Time.minutes(5))
      /** CountAgg 实现了 AggregateFunction 接口。功能是统计窗口中的条数，即遇到一条数据就加1
        * 聚合操作.aggregate(AggregateFunction af, WindowFunction wf)的第二个参数 WindowFunction 将每个 key 每个窗口聚合后的结果
        * 带上其他信息进行输出。我们这里实现的 WindowResultFunction 将 <主键商品 ID，窗口，点击量 >封装成了
        * ItemViewCount 进行输出。
        */
        .aggregate(new CountAgg(),new ItemCountWindowResult())
        .keyBy(_.windowEnd)
        .process(new TopNHotItems(3))

    env.execute("每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品")
  }
}

class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  //累加器设置起始值
  override def createAccumulator(): Long = 0L
  //累加器计算逻辑，此为+1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1
  //返回累加器
  override def getResult(acc: Long): Long = acc
  //合并累加器的值
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
//自定义窗口函数，输出ItemViewCount，第一个Long是预聚合最终输出的long，第三个long是ItemId
class ItemCountWindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNHotItems(topN:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",
      classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction
    [Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(i)
    //注册定时器， + 1 是延迟时间
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]
    #OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中的数据取出，放入到一个list buffer中
    val allItems:ListBuffer[ItemViewCount] = new ListBuffer()
    //将所有state中的数据取出，放入到一个list buffer中
    import scala.collection.JavaConversions._
    for (item <- itemState.get()){
      allItems += item
    }
    //按照Count大小排序，并取前N个 sortBy是升序
    val sortedItems = allItems.sortBy(_.clickCount)(Ordering.Long.reverse).take(topN)
    //清空状态
    itemState.clear()
    out.collect(sortedItems.toString())
    //将排名结果格式化输出 -1是之前注册定时器+1
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    //输出每一个商品信息
    for (i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append("商品ID =").append(currentItem.itemId)
        .append("浏览量").append(currentItem.clickCount)
        .append("\n")
    }
    result.append("===============")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}