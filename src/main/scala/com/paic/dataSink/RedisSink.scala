package com.paic.dataSink

import com.paic.transferFunction.Person
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-25 00:33
  *
  **/
object RedisSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
  }

  def redisSink(env:StreamExecutionEnvironment): Unit ={
    val dataSource: DataStream[String] = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt")
    val dataStream = dataSource.map(
      data => {
        val arr: Array[String] = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    //配置redis连接参数（即定义 FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
        .setHost("localhost")
        .setPort(6379)
        .build()

    dataStream.addSink(new RedisSink[Person](conf,new MyRedisMapper))



    env.execute("Sink To Redis")

  }
}

class MyRedisMapper extends RedisMapper[Person]{
  //定义保存数据写入redis的命令  HSET 表名 value key
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET,"person_score")

  //将分数指定为value
  override def getKeyFromData(data: Person): String = data.score.toString

  //将name指定为key
  override def getValueFromData(data: Person): String = data.name
}
