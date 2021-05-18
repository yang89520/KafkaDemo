package com.yiran.producer;


import com.yiran.ProducerTemplate;
import com.yiran.serializer.Person;
import com.yiran.serializer.PersonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * @program: BigdataSrc
 * @description: 序列化生产者
 * @author: Mr.Yang
 * @create: 2021-05-16 18:08
 **/
public class TheFourKafkaProducer implements ProducerTemplate {
    @Override
    public Properties setProperties() {
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
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", PersonSerializer.class);
        return properties;
    }

    @Override
    public void produceData(KafkaProducer producer) {
        Person person = new Person();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            person.setName("aa"+i);
            person.setSex("man"+i);
            person.setAge("10"+ random.nextInt());
            producer.send(new ProducerRecord("PersonSerializer","client"+i,person));
        }
    }
}








