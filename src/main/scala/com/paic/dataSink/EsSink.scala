package com.paic.dataSink

import java.util

import com.paic.transferFunction.Person
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-05-25 23:34
  *
  **/
object EsSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    esSink(env)

  }

  def esSink(env:StreamExecutionEnvironment): Unit ={
    val textData: DataStream[String] = env.readTextFile("E:\\itLearner\\FlinkEngine\\src\\main\\resources\\source.txt")
    val dataSource = textData.map(
      data => {
        val arr: Array[String] = data.split(" ")
        Person(arr(0),arr(1).toInt,arr(2))
      }
    )

    //定义HttpHost(问题：怎么写入注册多台ES主机
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.174.200",9200))

    //自定义写入es的EsSinkFunction
    val myEsSinkFunc: ElasticsearchSinkFunction[Person] = new ElasticsearchSinkFunction[Person] {
      override def process(t: Person, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装一个map或者一个json作为发送给es的数据源
        val sinkDataSource = new util.HashMap[String,String]()
        sinkDataSource.put("name",t.name)
        sinkDataSource.put("score",t.score.toString)
        sinkDataSource.put("motherland",t.motherland)

        //创建index request，用于发送http请求
        val indexRequest = Requests.indexRequest()
          .index("personInfo")
          .`type`("readingdata")
          .source(sinkDataSource)

        //用indexer发送请求
        requestIndexer.add(indexRequest)
      }
    }


    dataSource.addSink(new ElasticsearchSink
        .Builder[Person](httpHosts,myEsSinkFunc)
        .build()
    )




    env.execute("Data Sink To ElasticSearch")
  }

}
