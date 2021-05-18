package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {


        //1、指定参数
        Properties props = new Properties();

        //指定key的反序列化器
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //指定value的反序列化器
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //指定集群地址
        props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092");
        //指定消费者组id
        props.setProperty("group.id","g7");
        //指定消费组第一次消费的时候应该从哪里开始消费
        props.setProperty("auto.offset.reset","earliest");
        //是否自动提交offset
        props.setProperty("enable.auto.commit","false");


        //2、创建消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<String> topics = new ArrayList<String>();
        topics.add("second");
        //指定消费哪些topic
        consumer.subscribe(topics);
        //3、消费数据
        while(true){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();

            while (iterator.hasNext()){
                ConsumerRecord<String, String> record = iterator.next();
                long offset = record.offset();
                String topic = record.topic();
                int partition = record.partition();
                String message = record.value();
                System.out.println("topic="+topic+",partition="+partition+",offset="+offset+",message="+message);

                //手动提交[异步提交]
                consumer.commitAsync(new OffsetCommitCallback(){
                    //提交offset完成之后调用
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        Set<TopicPartition> keys = offsets.keySet();
                        for(TopicPartition tp: keys){
                            OffsetAndMetadata metadata = offsets.get(tp);

                            long commitOffset = metadata.offset();
                            System.out.println("提交的offset="+commitOffset);
                        }
                    }
                });
            }

        }

        //4、关闭


    }
}
