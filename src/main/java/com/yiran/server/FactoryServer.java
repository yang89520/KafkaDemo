package com.yiran.server;

import com.yiran.ConsumerTemplate;
import com.yiran.ProducerTemplate;
import com.yiran.consumer.TheFourKafkaConsumer;
import com.yiran.producer.TheFourKafkaProducer;
import com.yiran.producer.TheOneKafkaProducer;
import com.yiran.producer.TheThreeKafkaProducer;
import com.yiran.producer.TheTwoKafkaProducer;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: BigdataSrc
 * @description:
 * @author: Mr.Yang
 * @create: 2021-05-16 17:07
 **/

public class FactoryServer {


    private static Map<String, ProducerTemplate> produceMap=new HashMap<>();
    private static Map<String, ConsumerTemplate> consumerMap=new HashMap<>();

    public FactoryServer() {
        producerFactoryServer();
        consumerFactoryServer();
    }
    public static Map<String, ProducerTemplate> getProduceMap() {
        return produceMap;
    }

    public static Map<String, ConsumerTemplate> getConsumerMap() {
        return consumerMap;
    }
    /**
     * 生产factoryserver
     */
    public void producerFactoryServer(){
//        produceMap.put("demo1",new TheOneKafkaProducer());
//        produceMap.put("demo2",new TheTwoKafkaProducer());
//        produceMap.put("demo3",new TheThreeKafkaProducer());
        produceMap.put("demo4", new TheFourKafkaProducer());
    }
    public ProducerTemplate getProducer(String v){
        return produceMap.get(v);
    }


    /**
     * 消费者factoryserver
     */
    public void consumerFactoryServer(){
        consumerMap.put("demo4", new TheFourKafkaConsumer());
    }

    public ConsumerTemplate getConsumer(String v){
        return consumerMap.get(v);
    }


}
class M{
    public static void main(String[] args) {
        FactoryServer server = new FactoryServer();
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.getProducer("demo4").startRunning();
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                server.getConsumer("demo4").startRunning();
            }
        }).start();

//        server.getProducer("demo4").startRunning();
//        server.getConsumer("demo4").startRunning();
    }
}
