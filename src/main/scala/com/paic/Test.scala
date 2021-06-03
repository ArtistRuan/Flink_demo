package com.paic

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-17 01:00
  *
  **/
object Test {
  def main(args: Array[String]): Unit = {
    val str = "3,33,3333"
    val arr: Array[String] = str.split(",")

    println(arr(0))
    println(arr(1))
    println(arr(2))

    if(arr.size == 2){
      println("长度为2")
    } else {
      println("长度为3")
    }

    println(arr.size)
  }
}
