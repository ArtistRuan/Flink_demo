package com.paic.UDFTableApiSql

import com.paic.kafkaData.SensorTem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableAggregateFunction, TableFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  *
  * @program: FlinkEngine
  * @description: 分别使用table表函数和scala函数
  * @author: ruanshikao
  * @create: 2021-06-08 10:35
  *
  **/
object UdfApiSql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    udfApi(env)
//    udfSql(env)
//    udfApiTableFunction(env)
//    udfSqlTableFunction(env)
//    udfApiAggregateFunction(env)
//    udfSqlAggregateFunction(env)
    udfApiTableAggregateFunction(env)
  }
  //1-1UDF API udf函数
  def udfApi(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val textSourceStream: DataStream[String] = env.readTextFile(input)
    val sourceStream = textSourceStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
        override def extractTimestamp(t: SensorTem): Long = t.timestamp
      })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)

    //new UDF实例
    val udfHashCode = new myHashCode(23)
    //api依赖scala&java，故api table 可以直接调用UDF实例
    val apiResult = sourceTable
      .select('id,'ts,'temperature,udfHashCode('id))

    apiResult.toAppendStream[Row].print("api")
    env.execute()
  }
  //1-2UDF SQL udf函数
  def udfSql(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val textSourceStream = env.readTextFile(input)
    val sourceStream = textSourceStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)
    tableEnv.createTemporaryView("sourceTable",sourceTable)
    //new udf 实例
    val udfHashCode = new myHashCode(23)
    //sql调用udf需要注册UDF
    tableEnv.registerFunction("HashCode",udfHashCode)
    //sql中使用
    val sqlResult = tableEnv.sqlQuery(
      """
        |select id,ts,temperature,HashCode(id) from sourceTable
      """.stripMargin)

    sqlResult.toRetractStream[Row].print("sql")

    env.execute()
  }
  //2-1UDF table function 表函数 api
  def udfApiTableFunction(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceStream = env.readTextFile(input).map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
        override def extractTimestamp(t: SensorTem): Long = t.timestamp
      })

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)

    //new实例
    val udfSplit = new mySplit("_")
    //table api调用表函数
    val apiResult = sourceTable
        .joinLateral(udfSplit('id) as ('word,'word_len))
        .select('id,'ts,'word,'word_len)

    apiResult.toAppendStream[Row].print("table function api")
    env.execute()
  }
  //2-2UDF table function 表函数 sql
  def udfSqlTableFunction(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceStream = env.readTextFile(input).map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
          override def extractTimestamp(t: SensorTem): Long = t.timestamp
        })

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp.rowtime as 'ts,'temperature) //rowtime时间格式
    //注册表
    tableEnv.createTemporaryView("sourceTable",sourceTable)
    val udfSplit = new mySplit("_")
    //注册函数
    tableEnv.registerFunction("Split",udfSplit)
    //调用
    val sqlResult = tableEnv.sqlQuery(
      """
        |select
        | id,ts,temperature,word,word_len
        |from
        | sourceTable,lateral table( Split(id) ) as splitTable(word,word_len)
      """.stripMargin)

    sqlResult.toAppendStream[Row].print("sql表函数")

    env.execute()
  }
  //3-1UDF aggregate Function 聚合函数
  def udfApiAggregateFunction(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceStream = env.readTextFile(input).map(data => {
      val arr = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
        override def extractTimestamp(t: SensorTem): Long = t.timestamp
      })

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp.rowtime as 'ts,'temperature)

    //创建实例
    val myAvgTemp = new myAvgTemp()
    //table api
    val apiResult = sourceTable
        .groupBy('id)
        .aggregate(myAvgTemp('temperature) as 'avgTemp)
        .select('id,'avgTemp)

//    apiResult.toAppendStream[Row].print("api agg")
    apiResult.toRetractStream[Row].print("api agg")

    env.execute()
  }
  ////3-2UDF aggregate Function 聚合函数
  def udfSqlAggregateFunction(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceSream = env.readTextFile(input).map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
        override def extractTimestamp(t: SensorTem): Long = t.timestamp
      })

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceSream,'id,'timestamp as 'ts,'temperature)
    //注册表
    tableEnv.createTemporaryView("sourceTable",sourceTable)
    //注册函数
    val myAvgTemp = new myAvgTemp()
    tableEnv.registerFunction("myAvgTemp",myAvgTemp)
    //调用函数
    val sqlResult = tableEnv.sqlQuery(
      """
        |select
        | id,myAvgTemp(temperature)
        |from
        | sourceTable
        |group by
        | id
      """.stripMargin)

    //打印结果
//    sqlResult.toAppendStream[Row].print("sql agg") // 因是聚合，因为appStream不适合
    sqlResult.toRetractStream[Row].print("sql agg")

    env.execute()
  }
  //4.1UDF api table aggregate function
  def udfApiTableAggregateFunction(env:StreamExecutionEnvironment): Unit ={
    val input = "E:\\itLearner\\FlinkEngine\\src\\main\\resources\\sensor.txt"
    val sourceStream = env.readTextFile(input).map(data => {
      val arr: Array[String] = data.split(",")
      SensorTem(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorTem](Time.seconds(1)) {
        override def extractTimestamp(t: SensorTem): Long = t.timestamp
      })
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val sourceTable: Table = tableEnv.fromDataStream(sourceStream,'id,'timestamp as 'ts,'temperature)

    //注册
    val top2Temp = new Top2Temp()
    //table api
    val apiResult = sourceTable
        .groupBy('id)
        .flatAggregate(top2Temp('temperature) as ('temperature,'rank))
        .select('id,'temperature,'rank)

    apiResult.toRetractStream[Row].print("api table aggregate function")

    env.execute()
  }
  //4.2UDF sql table aggregate function(项目内容里面讲)
  def udfSqlTableAggregateFunction(env:StreamExecutionEnvironment): Unit ={
//    val sourceStream =
  }
}



//定义UDF函数：继承ScalarFunction,定义eval（s:String)方法
class myHashCode(factor:Int) extends ScalarFunction{
  def eval(s:String): Int ={
    //根据需求定义功能
    s.hashCode * factor - 10000
  }
}

//定义Table Function,实现TableFunction,定义求值eval方法
class mySplit(seperator:String) extends TableFunction[(String,Int)]{
  def eval(str:String): Unit ={
    str.split(seperator).foreach(
      //调用collect返回
      word => collect((word,word.length))
    )
  }
}
//定义状态
class AvgTempAcc{
  var sum : Double = 0.0
  var count : Int = 0
}

//自定义aggregate function,实现求平均温度,继承AggregateFunction，重写3个方法,
class myAvgTemp extends AggregateFunction[Double,AvgTempAcc]{
  //获取计算结果
  override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count
//创建更新器
  override def createAccumulator(): AvgTempAcc = new AvgTempAcc
//更新状态
  def accumulate( accumulator:AvgTempAcc,temp:Double): Unit ={
    accumulator.sum += temp
    accumulator.count += 1
  }
}

//定义一个类，用来表示表聚合函数的状态
class Top2TempAcc {
  var highestTemp : Double = Double.MinValue
  var secondHighestTemp : Double = Double.MinValue
}
//自定义表聚合函数，提取所有温度值中最高的两个温度，输出（temp,rank)
class Top2Temp extends TableAggregateFunction[(Double,Int),Top2TempAcc]{
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()
  //定义实现计算集合结果的函数 accumulate
  def accumulate(acc : Top2TempAcc, temp:Double): Unit ={
    if(temp > acc.highestTemp){
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp){
      acc.secondHighestTemp = temp
    }
  }
  //实现一个输出结果 emitValue的方法，最终处理表所有数据时调用
  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
    out.collect(acc.highestTemp,1)
    out.collect(acc.secondHighestTemp,2)
  }
}