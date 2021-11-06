//package com.paic.relationshipDataBaseData;
//
//import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
///**
// * @program: FlinkEngine
// * @description: ${description}
// * @author: ruanshikao
// * @create: 2021-06-11 14:31
// **/
//public class PostgreSqlSourceJava {
//    public static void main(String[] args) {
//
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//
//        String catalogName = "mycatalog";
//        String defaultDatabase = "postgres";
//        String username = "postgres";
//        String pwd = "postgres";
//        String baseUrl = "jdbc:postgresql://localhost:5432/";
//
////        PostgresCatalog postgresCatalog = (PostgresCatalog) JdbcCatalogUtils.createCatalog(
////                catalogName,
////                defaultDatabase,
////                username,
////                pwd,
////                baseUrl);
//
//
//        JdbcCatalog jdbcCatalog = new JdbcCatalog(catalogName, defaultDatabase, username, pwd, baseUrl);
//        bsTableEnv.registerCatalog("mycatelog", jdbcCatalog);
//
//        bsTableEnv.useCatalog("mycatelog");
//
//        Table table = bsTableEnv.sqlQuery("SELECT * FROM my_schema.table-name" );
//        DataStream<Row> rowDataStream = bsTableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();
//
//        try {
//            bsEnv.execute("test");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//}
