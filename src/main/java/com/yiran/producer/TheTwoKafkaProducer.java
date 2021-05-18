package com.yiran.producer;

import com.yiran.ProducerTemplate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @program: BigdataSrc
 * @description: 异步回调 实现onCompletion接口
 * send( topic,msg,new Callback() {
 *     public void onCompletion(RecordMetadata recordMetadata, Exception e) {}
 * })
 * @author: Mr.Yang
 * @create: 2021-05-16 17:10
 **/

public class TheTwoKafkaProducer implements ProducerTemplate<String,String> {

    public TheTwoKafkaProducer() {
        setProperties();
        produceData(getKafkaProducer());
        close();
    }

    @Override
    public Properties setProperties() {
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
        return properties;
    }

    @Override
    public void produceData(KafkaProducer<String, String> producer) {
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
    }

    @Override
    public void startRunning() {
        setProperties();
        produceData(getKafkaProducer());
        close();
    }
}
