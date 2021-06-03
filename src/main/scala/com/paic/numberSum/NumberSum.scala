package com.paic.numberSum

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-23 00:26
  *
  **/
object NumberSum {
  def main(args: Array[String]): Unit = {
//    sumFromStartToEnd_2(1,1)
    sumFromStartToEnd(2,3)

  }

  def sumFromStartToEnd(start: Int, end: Int): Unit = {
    var sum = 0
//    var number = 0

    for (number <- start to end) {
      sum += number
    }
    println("1-100累加结果为:" + sum)
  }

  def sumFromStartToEnd_1(start: Int, end: Int): Unit = {

    var num = 0
    var a = 0
    while (a <= 100) {
      num += a
      a += 1

    }
    println("1-100累加结果为:" + num)
  }

  def sumFromStartToEnd_2(start: Int, end: Int): Unit = {
    var num = 0
    for (a <- 1 to 100) {
      num += a
    }
    println("1-100累加结果为:" + num)
  }
}

