package com.yiran.producer;

import com.yiran.ProducerTemplate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @program: BigdataSrc
 * @description: 异步发送
 * @author: Mr.Yang
 * @create: 2021-05-16 17:10
 **/

public class TheOneKafkaProducer implements ProducerTemplate<String,String> {

    public TheOneKafkaProducer() {
    }

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
        int num=100;
        while(true){
            num++;
            producer.send(new ProducerRecord<String,String>("first",Integer.toString(num),
                    Integer.toString(num)));
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void startRunning() {
        setProperties();
        produceData(getKafkaProducer());
        close();
    }
}

//class Main{
//    public static void main(String[] args) {
//        TheOneKafkaProducer theOneKafkaProducer = new TheOneKafkaProducer();
//        theOneKafkaProducer.startRunning();
//    }
//}

