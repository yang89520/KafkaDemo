package com.yiran.producer;

import com.yiran.ProducerTemplate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @program: BigdataSrc
 * @description: 同步发送
 * Future对象的get方法线程阻塞,实际上是异步阻塞实现同步发送
 * get也可以设置超时时间
 * @author: Mr.Yang
 * @create: 2021-05-16 17:01
 **/

public class TheThreeKafkaProducer implements ProducerTemplate<String,String>{

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
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    @Override
    public void produceData(KafkaProducer<String, String> producer) {
        try {
            for (int i = 0; i < 100; i++) {
                Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>("three", "value-" + i));
                // get方法线程阻塞,实际上是异步阻塞实现同步发送
                RecordMetadata metadata = send.get();
                System.out.println("分区：" + metadata.partition() + " 偏移：" + metadata.offset());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void startRunning() {
        setProperties();
        produceData(getKafkaProducer());
        close();
    }
}







