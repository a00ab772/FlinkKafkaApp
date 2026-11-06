package com.example.flink.sink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkShoeRevenueJobWriter {

    public static void main(String[] args) throws Exception {
        // 1. Set up the Streaming Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create the Table Environment for Stream SQL
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 3. Register the 'shoes' Kafka table
        tableEnv.executeSql(
                "CREATE TABLE shoes (" +
                        "  id STRING," +
                        "  brand STRING," +
                        "  name STRING," +
                        "  sale_price DOUBLE," +
                        "  rowtime TIMESTAMP(3)," +
                        "  WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'shoes'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );

        // 4. Register the 'shoe_orders' Kafka table
        tableEnv.executeSql(
                "CREATE TABLE shoe_orders (" +
                        "  order_id BIGINT," +
                        "  product_id STRING," +
                        "  quantity INT," +
                        "  order_time TIMESTAMP(3)," +
                        "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'shoe_orders'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );

        // 5. Register the Upsert-Kafka sink table
        tableEnv.executeSql(
                "CREATE TABLE revenue_kafka_sink (" +
                        "  brand STRING," +
                        "  revenue DOUBLE," +
                        "  PRIMARY KEY (brand) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'upsert-kafka'," +
                        "  'topic' = 'revenue_by_brand'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'key.format' = 'json'," +
                        "  'value.format' = 'json'" +
                        ")"
        );

        // 6. Register PostgreSQL JDBC Sink Table
        tableEnv.executeSql(
                "CREATE TABLE revenue_postgres_sink (" +
                        "  brand STRING," +
                        "  revenue DOUBLE," +
                        "  PRIMARY KEY (brand) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'jdbc'," +
                        "  'url' = 'jdbc:postgresql://localhost:5432/shoeshop'," +
                        "  'table-name' = 'revenue_by_brand'," +
                        "  'username' = 'flinkuser'," +
                        "  'password' = 'flinkpassword'," +
                        "  'driver' = 'org.postgresql.Driver'" +
                        ")"
        );

        // 7. Create a reusable View for the latest shoe versions
        tableEnv.executeSql(
                "CREATE VIEW latest_shoes AS " +
                        "SELECT * FROM (" +
                        "  SELECT *," +
                        "    ROW_NUMBER() OVER (PARTITION BY id ORDER BY rowtime DESC) AS rownum" +
                        "  FROM shoes" +
                        ") " +
                        "WHERE rownum = 1"
        );

        // 8. Execute INSERT statements to write to BOTH sinks simultaneously using Statement Set
        String query =
                "SELECT brand, ROUND(SUM(sale_price * quantity) / 100.00, 2) AS revenue " +
                        "FROM shoe_orders " +
                        "JOIN latest_shoes " +
                        "ON shoe_orders.product_id = latest_shoes.id " +
                        "GROUP BY brand";

        // Flink Statement Set allows executing multiple INSERTs as a single pipeline job efficiently
        org.apache.flink.table.api.StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO revenue_kafka_sink " + query);
        statementSet.addInsertSql("INSERT INTO revenue_postgres_sink " + query);
        statementSet.execute();
    }
}