package com.paic

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-24 21:50
  *
  **/
object YangLiLei {

}
//包装样例类
case class Student(name:String,age:Int,score:Double,address:String)

case class EpmsGoods(sku:String,price:Double,upTime:String)

class myMap extends MapFunction[Student,Person]{
  override def map(value: Student): Person = Person(value.name,value.age,value.address)
}

class myRichMap extends RichMapFunction[Student,Person]{

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def map(value: Student): Person = Person(value.name,value.age,value.address)

  override def close(): Unit = super.close()
}
////SensorReading(id,temperature,timstamp)
//case class SensorReading(id:String,temperature:Double,timestamp:Double)

