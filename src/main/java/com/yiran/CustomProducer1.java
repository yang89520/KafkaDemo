package com.yiran;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @program: KafkaDemo
 * @description: 带回调的生产者api
 * @author: Mr.Yang
 * @create: 2021-05-06 11:49
 **/

public class CustomProducer1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092");
        properties.put("acks","all");
        properties.put("retries",1);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        System.out.println("success->"+recordMetadata.offset());
                        System.out.println(recordMetadata.toString());
                        System.out.println(recordMetadata.partition());
                        System.out.println(recordMetadata.serializedKeySize());
                        System.out.println(recordMetadata.serializedValueSize());
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.close();;
    }
}
