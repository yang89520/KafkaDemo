package com.yiran.consumer;

import com.yiran.ConsumerTemplate;
import com.yiran.serializer.Person;
import com.yiran.serializer.PersonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @program: BigdataSrc
 * @description: 序列化消费者
 * @author: Mr.Yang
 * @create: 2021-05-16 22:51
 **/

public class TheFourKafkaConsumer implements ConsumerTemplate<String, Person> {
    @Override
    public Properties setProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        //指定消费组第一次消费的时候应该从哪里开始消费
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", PersonDeserializer.class.getName());
        return props;
    }
    @Override
    public void consumeData(KafkaConsumer<String,Person> consumer) {
        consumer.subscribe(Arrays.asList("PersonSerializer"));
        ConsumerRecords<String,Person> records = consumer.poll(Duration.ofMillis(1000));
        records.forEach((record)->{
            System.out.println("key:"+record.key()+"  value:"+record.value().toString());
        });

    }
}

