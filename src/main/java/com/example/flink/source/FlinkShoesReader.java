package com.example.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkShoesReader {

    public static void main(String[] args) throws Exception {
        // 1. Set up the Flink streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Configure the Kafka Source using Flink's modern connector API
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092") // Use broker:29092 if running inside the Docker network
                .setTopics("shoes")
                .setGroupId("flink-shoes-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Create the data stream from the Kafka source with a basic watermark strategy
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka-Shoes-Source")
                .print(); // Prints each incoming JSON string to standard output

        // 4. Execute the Flink job
        env.execute("Read Shoes Topic from Kafka");
    }
}