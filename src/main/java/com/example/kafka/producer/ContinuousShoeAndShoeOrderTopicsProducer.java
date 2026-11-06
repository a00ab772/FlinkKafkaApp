package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class ContinuousShoeAndShoeOrderTopicsProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Use broker:29092 if running inside the Docker network
    private static final String TOPIC_SHOES = "shoes";
    private static final String TOPIC_ORDERS = "shoe_orders";

    private static final String[][] SHOES_CATALOG = {
            {"1db091e5-c516-4286-84a2-8d687d6c0331", "Williamson Group", "Impreza Pro 634", "7995.0"},
            {"2ef832f6-d727-5397-95b3-9e788e7d8f42", "Nike", "Air Max 270", "150.0"},
            {"3fc943a7-e838-6408-a6c4-0f899f8e9g53", "Adidas", "Ultraboost Light", "190.0"},
            {"4ad054b8-f949-7519-b7d5-1f900a9f0h64", "Puma", "RS-X Efekt", "110.0"},
            {"5be165c9-a05a-8620-c8e6-2g011b0a1i75", "New Balance", "Fresh Foam 1080", "165.0"}
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault());

        long orderIdCounter = 200L;

        System.out.println("Starting continuous event production to Kafka... Press Ctrl+C to stop.");

        try {
            while (true) {
                String currentTime = formatter.format(Instant.now());

                // 1. Send a random record to the shoes catalog
                int shoeIndex = random.nextInt(SHOES_CATALOG.length);
                String[] shoe = SHOES_CATALOG[shoeIndex];
                String shoeJson = String.format(
                        "{\"id\": \"%s\", \"brand\": \"%s\", \"name\": \"%s\", \"sale_price\": %s, \"rowtime\": \"%s\"}",
                        shoe[0], shoe[1], shoe[2], shoe[3], currentTime
                );
                producer.send(new ProducerRecord<>(TOPIC_SHOES, shoe[0], shoeJson));
                System.out.println("Sent to [shoes]: " + shoeJson);

                // 2. Send an associated order (shoe_orders)
                String orderJson = String.format(
                        "{\"order_id\": %d, \"product_id\": \"%s\", \"quantity\": %d, \"order_time\": \"%s\"}",
                        orderIdCounter++, shoe[0], random.nextInt(3) + 1, currentTime
                );
                producer.send(new ProducerRecord<>(TOPIC_ORDERS, orderJson));
                System.out.println("Sent to [shoe_orders]: " + orderJson);

                // Wait 2 seconds between each send to simulate real-time streaming
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            System.err.println("Producer interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            producer.close();
        }
    }
}