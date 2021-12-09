package com.paic.analysizData.dataPipeLine

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description: PG -> MYSQL
  *               CREATE TABLE "public"."almart_all" (
  *               "date_key" date,
  *               "hour_key" int2,
  *               "client_key" int4,
  *               "item_key" int4,
  *               "account" int4,
  *               "expense" numeric
  *               )
  *               ;
  *
  *               ALTER TABLE "public"."almart_all"
  *               OWNER TO "postgres";
  *               ------------------------------------
  *               CREATE TABLE almart_all (
  *               date_key date,
  *               hour_key int2,
  *               client_key int4,
  *               item_key int4,
  *               account int4,
  *               expense numeric
  *               );
  * @author: ruanshikao
  * @create: 2021-12-09 15:05
  *         开发状态：已经是可以执行，但数据量大，执行慢。
  *
  **/
//输入样例类
case class LocalPgSourceAlmartAll20211209(date_key:String,hour_key:Int,client_key:Int,item_key:Int,account:Int,expense:Double)
//输出样例类
case class LocalMysqlSinkAlmartAll20211209(date_key:String,hour_key:Int,client_key:Int,item_key:Int,account:Int,expense:Double)
object LocalPg2LocalMysql_20211209 {
  def main(args: Array[String]): Unit = {

    main_df()

  }

  def main_df(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    println("这1")
    val sourcePg: DataStream[LocalPgSourceAlmartAll20211209] = env.addSource(new LocalPg2LocalMysqlSource())
    println("这2")
    //打印一下数据
    sourcePg.print()
    println("这3")
    //将数据sink到mysql
    val resul = sourcePg.map(
      data => {
        LocalMysqlSinkAlmartAll20211209(data.date_key,data.hour_key,data.client_key,data.item_key,data.account,data.expense)
      }
    )
    resul.addSink(new LocalPg2MysqlSink())
    println("这4")
    env.execute()
    println("这5")
  }
}

//pg数据到样例类
class LocalPg2LocalMysqlSource() extends RichSourceFunction[LocalPgSourceAlmartAll20211209]{
  //获取连接
  var conn:Connection = _
  var pstmt:PreparedStatement = _
  //执行的状态
  var running = true

  override def open(parameters: Configuration): Unit = {
    //设置连接信息
    conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres","Paic1234")
    pstmt = conn.prepareStatement("select * from almart_all")
  }

  override def close(): Unit = {
    pstmt.close()
    conn.close()
  }

  override def run(sourceContext: SourceFunction.SourceContext[LocalPgSourceAlmartAll20211209]): Unit = {
    //查询加载数据接入样例类
    val res: ResultSet = pstmt.executeQuery()
    while (res.next()){
      sourceContext.collect(
        LocalPgSourceAlmartAll20211209(
          res.getDate("date_key").toString,
          res.getInt("hour_key"),
          res.getInt("client_key"),
          res.getInt("item_key"),
          res.getInt("account"),
          res.getInt("expense")
        )
      )
    }
  }

  override def cancel(): Unit = running = false
}

//流数据到mysql
class LocalPg2MysqlSink() extends RichSinkFunction[LocalMysqlSinkAlmartAll20211209]{
  //设置连接
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
//  var upsertStmt:PreparedStatement = _
  override def invoke(value: LocalMysqlSinkAlmartAll20211209, context: SinkFunction.Context[_]): Unit = {
    insertStmt.setString(1,value.date_key)
    insertStmt.setInt(2,value.hour_key)
    insertStmt.setInt(3,value.client_key)
    insertStmt.setInt(4,value.item_key)
    insertStmt.setInt(5,value.account)
    insertStmt.setDouble(6,value.expense)
    //执行语句
    insertStmt.execute()
  }

  override def open(parameters: Configuration): Unit = {
    //1、设置连接，定义预编译语句
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into almart_all (date_key,hour_key,client_key,item_key,account,expense) values (?,?,?,?,?,?)")
//    upsertStmt = conn.prepareStatement("update  set")
  }

  override def close(): Unit = {
    insertStmt.close()
//    upsertStmt.close()
    conn.close()
  }
}