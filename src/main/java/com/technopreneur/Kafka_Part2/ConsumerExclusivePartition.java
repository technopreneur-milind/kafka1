package com.technopreneur.Kafka_Part2;

//This is yet to be implemented
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerExclusivePartition {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group-fix");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(properties);
        TopicPartition p1 = new TopicPartition("demo", 0);
        TopicPartition p3 = new TopicPartition("demo", 2);
        TopicPartition p5 = new TopicPartition("demo", 4);
        kafkaConsumer.assign(Arrays.asList(p1,p3,p5));
        try{
            while (true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord<String,String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}
