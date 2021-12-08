package com.paic.relationshipDataBaseData

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
  *
  * @program: FlinkEngine
  * @description: ${description}
  * @author: ruanshikao
  * @create: 2021-06-09 11:28
  *
  **/
object PostgreSqlSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    JdbcPg(env)
  }
  def JdbcPg(env:StreamExecutionEnvironment): Unit = {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    val name            = "mypg"
    val defaultDatabase = "postgres"
    val username        = "postgres"
    val password        = "Paic1234"
    val baseUrl         = "jdbc:postgresql://localhost:5432/postgres"

    val catalog: JdbcCatalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl)
    tableEnv.registerCatalog("mypg", catalog)

    // set the JdbcCatalog as the current catalog of the session
    tableEnv.useCatalog("mypg")

    val resultPg = tableEnv.sqlQuery(
      """
        |select * from `public.company`
      """.stripMargin)

    resultPg.printSchema()

//    resultPg.toAppendStream[Row].print()

    env.execute()
  }


}



