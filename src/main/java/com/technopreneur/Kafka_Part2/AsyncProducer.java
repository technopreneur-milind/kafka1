package com.technopreneur.Kafka_Part2;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducer {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        try{
            for(int i = 0; i < 30; i++){
                System.out.println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(Calendar.getInstance().getTime()) +" Sending -> " + i);
                kafkaProducer.send(new ProducerRecord("demo", Integer.toString(i), "test message - " + i ), new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						
						 System.out.println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(Calendar.getInstance().getTime())+" got -> " + metadata);
					}
				});
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
