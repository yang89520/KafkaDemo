package com.yiran;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * kafka-topic模板接口
 * @param <K>
 * @param <V>
 */
public interface ProducerTemplate<K,V> {

    /**
     * 配置项
     * @return
     */
    Properties setProperties();

    /**
     * 获取KafkaProducer对象
     * @return
     */
    default KafkaProducer<K,V> getKafkaProducer(){
        Properties prop = setProperties();
        KafkaProducer<K, V> producer = new KafkaProducer<K, V>(prop);
        return producer;
    }

    /**
     * 数据发送的逻辑
     * @param producer
     */
    void produceData(KafkaProducer<K, V> producer);

    /**
     * kafka数据流关闭的逻辑
     */
    default void close(){
        getKafkaProducer().close();
    };

    default void startRunning(){
        setProperties();
        produceData(getKafkaProducer());
        close();
    };
}




