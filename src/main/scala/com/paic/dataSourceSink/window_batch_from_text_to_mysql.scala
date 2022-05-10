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
  * @description: 读取文件数据，并开窗批量插入数据库mysql
  *              有时候异常退出，但没有报错信息，
  *              有时候执行成功，但是没有数据入库
  *              有时候执行成功，有数据入库，速度10-30条数据/秒
  *              待分析处理原因!!!
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
    var count = 1
    val source_steam: DataStream[Window_batch_from_text_to_mysql_cc] = env.readTextFile(data_path)
      .map(
        data => {
          val arr: Array[String] = data.split(",")
          println("这里" + count + "次")
          count += 1  // 文件里有485730条数据，跑这里121426次/121431次/121444次/121426次/121429次
          Window_batch_from_text_to_mysql_cc(arr(0), arr(1), arr(2), arr(3), arr(4).toLong)
        }
      )

    // 开窗处理数据
    source_steam.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction[Window_batch_from_text_to_mysql_cc,Iterable[Window_batch_from_text_to_mysql_cc],TimeWindow] {
      override def apply(window: TimeWindow, input: Iterable[Window_batch_from_text_to_mysql_cc], out: Collector[Iterable[Window_batch_from_text_to_mysql_cc]]): Unit = {
        println("开窗里面的内容!!!")  // 这里的内容没有打印
        if(input.nonEmpty){
//          System.out.print("1分钟内收集到 visit_list 的数据条数是：" + visits_list.size())
          println("1分钟内收集到 visit_list 的数据条数是：" + input.size)  // 这里的内容没有打印
          out.collect(input)
        }

      }
    })
    .addSink(new MyWindowSink)   // 将数据加载到mysql
    env.execute("stream job")
  }
}

class MyWindowSink extends RichSinkFunction[Iterable[Window_batch_from_text_to_mysql_cc]]{
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink","root","123456")
    //定义预编译语句
    insertStmt = conn.prepareStatement("insert into webview_via_table_api values (?,?,?,?,?)")
  }

  override def close(): Unit = {
    if(conn != null){
      conn.close()
    }
    if(insertStmt != null){
      insertStmt.close()
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
      insertStmt.setString(1,row.web_id)
      insertStmt.setString(2,row.city_id)
      insertStmt.setString(3,row.user_id)
      insertStmt.setString(4,row.action_name)
      insertStmt.setLong(5,row.action_time)
//      ps.addBatch(a1 + "," + a2 + "," + a3 + "," + a4 + "," + a5)
      insertStmt.addBatch()

    })

    val count = insertStmt.executeBatch()
    conn.commit()

    println("成功写入数据条数: " + count.length)

  }
}