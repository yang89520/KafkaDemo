package com.yiran;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public interface ConsumerTemplate<K,V> {
    Properties setProperties();

    default KafkaConsumer<K,V> getKafkaConsumer(){
        Properties prop = setProperties();
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(prop);
        return consumer;
    }

    void consumeData(KafkaConsumer<K, V> consumer);

    default void close(){ getKafkaConsumer().close();}

    default void startRunning(){
        setProperties();
        consumeData(getKafkaConsumer());
        close();
    };
}

class TheAutoOffsetConsumer implements ConsumerTemplate<String,String> {



    @Override
    public Properties setProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    /**
     * 简单的打印出了偏移量和kv
     * @param consumer
     */
    @Override
    public void consumeData(KafkaConsumer<String,String> consumer) {
        consumer.subscribe(Arrays.asList("topicA","topicB"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }
    }

    @Override
    public void startRunning() {
        consumeData(getKafkaConsumer());
        close();
    }
}

class TheManualOffsetConsumer implements ConsumerTemplate<String,String> {

    @Override
    public Properties setProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void consumeData(KafkaConsumer<String, String> consumer) {

    }

    @Override
    public void startRunning() {

    }
}


/**
 * 实现消费者端精准一次消费demo
 * Auther：xcoder

public class ConsumerRebalanceListenerApp {
    public static void main(String[] args) {
        FileInputStream fileInputStream = new FileInputStream("");

        // 基本的配置项
        Properties properties = new Properties();
        properties.load(fileInputStream);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 禁止自动提交消费位移的功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 如果分区没有初始偏移量，或者当前偏移量服务器上不存在时，将使用的偏移量设置，
        // earliest从头开始消费，latest从最近的开始消费，none抛出异常
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        // 用于保存最新的消息消费位置，以防止其未提交
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton("test-topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    OffsetAndMetadata offset = consumer.committed(partition);
                    consumer.seek(partition, offset.offset());
                }

                offsets.clear();
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            Set<TopicPartition> partitions = records.partitions();
            partitions.forEach(partition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                partitionRecords.forEach(record -> {
                    // 消息处理逻辑
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                    // 保存消息的位移信息
                    OffsetAndMetadata offset = offsets
                            .computeIfAbsent(partition, x -> new OffsetAndMetadata(record.offset()));
                    if (offset.offset() < record.offset()) {
                        offsets.put(partition, new OffsetAndMetadata(record.offset()));
                    }

                    // 这里实现的是精确一次的语义，即每消费一条消息就提交该消息的位移信息，这里粒度也可以更粗一些，
                    // 就是在当前循环外一次性提交当前partition的最新的offset，但是这样有可能出现消费到中间位置断开的情况
                    consumer.commitSync(offsets);
                });
            });
        }
    }
}
*/

