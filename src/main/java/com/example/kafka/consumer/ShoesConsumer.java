package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ShoesConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Use broker:29092 if running inside Docker
    private static final String TOPIC_SHOES = "shoes";
    private static final String GROUP_ID = "shoes-consumer-group-1";

    public static void main(String[] args) {
        // Configure Kafka Consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 'earliest' ensures it reads from the beginning of the topic if no offset is committed yet
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the shoes topic
        consumer.subscribe(Collections.singletonList(TOPIC_SHOES));

        System.out.println("Starting Kafka consumer for [shoes]... Listening for catalog messages. Press Ctrl+C to exit.");

        try {
            while (true) {
                // Poll for new data from Kafka (timeout every 1 second)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received Shoe Catalog Item -> Partition: %d | Offset: %d | Key: %s | Value: %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        } finally {
            consumer.close(); // Close the consumer safely on exit
        }
    }
}