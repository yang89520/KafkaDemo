package com.atguigu.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        //1、指定生产者参数
        Properties props = new Properties();

        //指定key的序列化器
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化器
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //指定ack
        props.setProperty("acks","1");
        //指定kafka集群地址
        props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //指定批次大小
        props.setProperty("batch.size","1024");

        //props.setProperty("transactional.id","t001");

        //2、创建生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //3、封装数据
        for(int i=100;i<=199;i++){
            System.out.println("开始发送第"+i+"条数据");
            final int index = i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("second", "这是第" + i + "条消息");
            //4、发送
            //异步发送
            /*producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("返回第"+index+"条确认消息");
                    String topic = metadata.topic();
                    int partition = metadata.partition();
                    long offset = metadata.offset();
                    System.out.println("topic="+topic+",partition="+partition+",offset="+offset+","+index);
                }
            });*/
            //同步发送
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("返回第"+index+"条确认消息");
                    String topic = metadata.topic();
                    int partition = metadata.partition();
                    long offset = metadata.offset();
                    System.out.println("topic="+topic+",partition="+partition+",offset="+offset+","+index);
                }
            }).get();
            System.out.println("第"+i+"条数据发送完成");
        }


        //5、关闭
        producer.close();
    }
}
