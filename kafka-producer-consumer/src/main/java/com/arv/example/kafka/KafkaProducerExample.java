package com.arv.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // Kafka properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your broker
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Create and send a message
        String topic = "engagement-topic"; // Replace with your topic
        String key = "key1";
        String value = "Hello, Kafka3!";
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // Get metadata for the sent record
            System.out.println("Message sent to topic " + metadata.topic() + 
                               " partition " + metadata.partition() + 
                               " with offset " + metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close(); // Always close the producer
        }
    }
}
