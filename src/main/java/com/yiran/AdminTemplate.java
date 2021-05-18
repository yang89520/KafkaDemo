package com.yiran;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public interface AdminTemplate {

    Properties setProperties();

    default AdminClient connectKafka(){
        Properties prop = setProperties();
        AdminClient client = AdminClient.create(prop);
        return client;
    }


    void topicContorller(AdminClient adminClient);

    /**
     * 定义topic连接对象创建
     */
    void topicRunning();


}
