package com.yiran;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @program: KafkaDemo
 * @description: 不带回调的生产者
 * @author: Mr.Yang
 * @create: 2021-05-06 11:18
 **/

public class CustomProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        //kafka集群，broker-list
        properties.put("bootstrap.servers","hadoop102:9092");
        properties.put("ack","all");
        //重试次数
        properties.put("retries",1);
        //批次大小
        properties.put("batch.memory",16384);
        //等待时间 ack返回等待的时长
        properties.put("linger.ms",1);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory",33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer(properties);
//        for (int i = 0; i <100 ; i++) {
//            producer.send(new ProducerRecord<String,String>("first",Integer.toString(i),
//                    Integer.toString(i)));
//        }
        //producer.close();
        int num=100;
        while(true){
            num++;
            producer.send(new ProducerRecord<String,String>("first",Integer.toString(num),
                    Integer.toString(num)));
            Thread.sleep(3000);
        }

    }
}
