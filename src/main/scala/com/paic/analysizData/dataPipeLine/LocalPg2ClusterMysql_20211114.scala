package com.paic.analysizData.dataPipeLine

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.paic.dataSourceSink.PgSource
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  *
  * @program: FlinkEngine
  * @description:
  *              本地pg数据通过数据管道推送到远程mysql数据库
                 select count(1) from almart_all;  --521.372s  300000000 rows
  * @author: ruanshikao
  * @create: 2021-11-14 18:28
  * update_time: 2021-12-08 20:28
  *
  **/
//输入数据样例类
case class LocalPgPostgresAlmartAll(date_key:String,hour_key:Int,client_key:String,item_key:Int,account:Int,expense:Int)

object LocalPg2ClusterMysql_20211114 {
  def main(args: Array[String]): Unit = {
    //流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(4)
    //设置重启
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,60000L))

    billionPg2MysqlBySQL(env)
    env.execute("billionPg2MysqlBySQL")
  }

  /**
    * 对于数据库操作，flink-sql与API
    * @param env
    */
  def billionPg2MysqlBySQL(env:StreamExecutionEnvironment): Unit ={
    val sourceStream: DataStream[LocalPgPostgresAlmartAll] = env.addSource(new LocalPgAddSource())

    sourceStream.print()

//    env.execute()

  }

  /**
    * flink-api
    * @param env
    */
  def billionPg2MysqlByAPI(env:StreamExecutionEnvironment): Unit ={

  }
}

class LocalPgAddSource() extends RichSourceFunction[LocalPgPostgresAlmartAll]{
  //定义驱动
  var conn:Connection = _
  var pstmt:PreparedStatement = _
  var running = true
  var sql = "select * from almart_all limit 10"
  override def open(parameters: Configuration): Unit = {
    //设置数据库连接
    conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres","Paic1234")
    pstmt = conn.prepareStatement(sql)
  }

  override def close(): Unit = {
    pstmt.close()
    conn.close()
  }

  override def run(sourceContext: SourceFunction.SourceContext[LocalPgPostgresAlmartAll]): Unit = {
    //获取数据，加载到样例类
    val res = pstmt.executeQuery()
    while(res.next()){
      sourceContext.collect(
        LocalPgPostgresAlmartAll(
          res.getDate("date_key").toString,
          res.getInt("hour_key"),
          res.getInt("client_key").toString,
          res.getInt("item_key"),
          res.getInt("account"),
          res.getInt("expense")
        ))
    }
  }

  override def cancel(): Unit = running = false
}