package com.paic.dataSourceSink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



/**
  *
  * @program: FlinkEngine
  * @description: 读取文件数据，并开窗批量插入数据库mysql（执行完成，但没有数据更新，待分析处理）
  * @author: ruanshikao
  * @create: 2022-05-09 22:01
  *
  **/

case class Window_batch_from_text_to_mysql_cc(web_id:String,city_id:String,user_id:String,action_name:String,action_time:Long)

object window_batch_from_text_to_mysql {
  def main(args: Array[String]): Unit = {


    // 创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 加载数据
    val data_path = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\UserBehavior.dat"
    val source_steam: DataStream[Window_batch_from_text_to_mysql_cc] = env.readTextFile(data_path)
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          Window_batch_from_text_to_mysql_cc(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
        }
      )

    // 开窗处理数据
    source_steam.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction[Window_batch_from_text_to_mysql_cc,Iterable[Window_batch_from_text_to_mysql_cc],TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[Window_batch_from_text_to_mysql_cc], out: Collector[Iterable[Window_batch_from_text_to_mysql_cc]]): Unit = {

        if(input.nonEmpty){
//          System.out.print("1分钟内收集到 visit_list 的数据条数是：" + visits_list.size())
          println("1分钟内收集到 visit_list 的数据条数是：" + input.size)
          out.collect(input)
        }
      }
    })
    // 将数据加载到mysql
      .addSink(new MyWindowSink)
    env.execute("stream job")
  }
}

class MyWindowSink extends RichSinkFunction[Iterable[Window_batch_from_text_to_mysql_cc]]{
  var conn: Connection = _
  var ps: PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink","root","123456")
    //定义预编译语句
    val ps: PreparedStatement = conn.prepareStatement("insert into webview_via_table_api values (?,?,?,?,?)")
  }

  override def close(): Unit = {
    if(conn != null){
      conn.close()
    }
    if(ps != null){
      ps.close()
    }

  }

  override def invoke(value: Iterable[Window_batch_from_text_to_mysql_cc], context: SinkFunction.Context[_]): Unit = {
//    val dataList = value.toList
//    ps = conn.prepareStatement("")
//    dataList.foreach(sdkData => {
//      val category = sdkData.key
//      val sql = ParseUtils.getSqlStr(category)
//      sql.append(setPreparedStatement(sdkData.data, category))
//      ps.addBatch(sql.toString)}

    val dataList = value.toList
//    ps = conn.prepareStatement("")


    dataList.foreach(row => {
//      val a1 = ps.setString(1,row.web_id)
//      val a2 = ps.setString(2,row.city_id)
//      val a3 = ps.setString(3,row.user_id)
//      val a4 = ps.setString(4,row.action_name)
//      val a5 = ps.setLong(5,row.action_time)
      ps.setString(1,row.web_id)
      ps.setString(2,row.city_id)
      ps.setString(3,row.user_id)
      ps.setString(4,row.action_name)
      ps.setLong(5,row.action_time)
//      ps.addBatch(a1 + "," + a2 + "," + a3 + "," + a4 + "," + a5)
      ps.addBatch()
    })

    val count = ps.executeBatch()
    conn.commit()

    println("成功写入数据条数: " + count.length)

  }
}