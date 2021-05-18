package com.yiran.admin;


import com.yiran.AdminTemplate;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * @program: KafkaDemo
 * @description:
 * @author: Mr.Yang
 * @create: 2021-05-09 14:24
 **/


public class SimpleAdmin implements AdminTemplate {

    public  String TOPIC_NAME="idea1_topic";

    public SimpleAdmin() {
    }

    public SimpleAdmin(String topic_name) {
        TOPIC_NAME=topic_name;
    }

    public void setTOPIC_NAME(String TOPIC_NAME) {
        this.TOPIC_NAME = TOPIC_NAME;
    }

    @Override
    public Properties setProperties() {
        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        return prop;
    }

    @Override
    public void topicContorller(AdminClient adminClient) {
       // adminClient;
    }


    /**
     * 创建topic
     * @param conn
     */
    public  void createTopic(AdminClient conn){
        //定义副本数
        Short rs = 3;
        NewTopic topic = new NewTopic(TOPIC_NAME, 1, rs);
        // 创建topic
        CreateTopicsResult topics = conn.createTopics(Arrays.asList(topic));
        System.out.println("CreateTopicResult:"+topics.toString());
    }

    /**
     * 创建topic
     * @param conn
     */
    public  void createTopic(AdminClient conn,final String topicName){
        //定义副本数
        Short rs = 3;
        NewTopic topic = new NewTopic(topicName, 1, rs);
        // 创建topic
        CreateTopicsResult topics = conn.createTopics(Arrays.asList(topic));
        System.out.println("CreateTopicResult:"+topics.toString());
    }

    /**
     * 查看所有topic
     * @param conn
     */
    public void listTopic(AdminClient conn){
        try {

            //ListTopicsResult listTopics = conn.listTopics();
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); //打印出内部的topic
            ListTopicsResult listTopics = conn.listTopics(options);
            // topic name
            Set<String> names = listTopics.names().get();
            // 监听状态
            Collection<TopicListing> listings = listTopics.listings().get();
            // {name:监听状态}
            Map<String, TopicListing> namesToListings = listTopics.namesToListings().get();
            //打印names for-each
            names.stream().forEach(System.out::println);
            listings.stream().forEach(System.out::println);
            namesToListings.keySet().stream().forEach(System.out::println);
            namesToListings.values().stream().forEach(System.out::println);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除topic
     * @param conn
     */
    public void deleteTopic(AdminClient conn){
        DeleteTopicsResult deleteTopicsResult = conn.deleteTopics(Arrays.asList(TOPIC_NAME));
        try {
            deleteTopicsResult.all().get();
            listTopic(conn);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    /**
     * 描述topic
     * nameidea1_topicval(
     * name=idea1_topic,
     * internal=false,
     * partitions=(
     *          partition=0,
     *          leader=hadoop104:9092 (id: 2 rack: null),
     *          replicas=hadoop104:9092 (id: 2 rack: null),
     *          hadoop103:9092 (id: 1 rack: null),
     *          hadoop102:9092 (id: 0 rack: null),
     *          isr=hadoop104:9092 (id: 2 rack: null),
     *          hadoop103:9092 (id: 1 rack: null),
     *          hadoop102:9092 (id: 0 rack: null)),
     *          authorizedOperations=[]
     *          )
     * @param conn
     */
    public void describeTopic(AdminClient conn){
        DescribeTopicsResult describeTopicsResult = conn.describeTopics(Arrays.asList(TOPIC_NAME));
        try {
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            Set<Map.Entry<String, TopicDescription>> set = stringTopicDescriptionMap.entrySet();
            set.stream().forEach((entry)->{
                System.out.println("name"+entry.getKey()+"val"+entry.getValue());
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看kafka配置项，TODO 待增加方法参数，实现自助查询功能
     * @param conn
     */
    public void describeConfig(AdminClient conn){
        //ConfigResource conf = new ConfigResource(ConfigResource.Type.BROKER, TOPIC_NAME);
        ConfigResource conf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = conn.describeConfigs(Arrays.asList(conf));
        try {
            Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
            Set<Map.Entry<ConfigResource, Config>> set = configResourceConfigMap.entrySet();
            set.stream().forEach((entry)->{
                System.out.println("name:"+entry.getKey());
                Collection<ConfigEntry> entries = entry.getValue().entries();
                entries.stream().forEach((configEntry)->{
                    if ("preallocate".equals(configEntry.name())){
                        System.out.println(configEntry.name()+"  "+configEntry.value());
                    }
                });
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改kafka的conf并查看修改结果
     * @param conn
     */
    public void alterConfig(AdminClient conn){
        Map<Object, Object> configMap = new HashMap<>();
        ArrayList<Object> confList = new ArrayList<>();
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));
        AlterConfigOp configOp = new AlterConfigOp(new ConfigEntry("preallocate", "true"), AlterConfigOp.OpType.SET);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        confList.add(configOp);
        configMap.put(configResource, (Collection)confList);
        AlterConfigsResult alterConfigsResult = conn.incrementalAlterConfigs((Map)configMap);
        describeConfig(conn);
        try {
            alterConfigsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 增加分区
     * @param conn
     * @param patition
     */
    public void incrPartitions(AdminClient conn,Integer patition){
        //partitionmap
        Map<Object, Object> partitionsMap = new HashMap<>();
        //增加分区
        //int totalCount, List<List<Integer>> newAssignments
        NewPartitions newPartitions = NewPartitions.increaseTo(patition);
        partitionsMap.put(TOPIC_NAME,newPartitions);
        //查询分区信息
        describeTopic(conn);
    }


    @Override
    public void topicRunning() {
        //createTopic(connectKafka());
        createTopic(connectKafka(),"PersonSerializer");
//        listTopic(connectKafka());
//        //deleteTopic(connectKafka());
//        describeTopic(connectKafka());
//        describeConfig(connectKafka());
//        alterConfig(connectKafka());
//        incrPartitions(connectKafka(),2);

        Stream.of(1,2,3,4).map(t->t+=1).forEach(System.out::println);

    }
}

class Main{
    public static void main(String[] args) {
        SimpleAdmin simpleAdmin = new SimpleAdmin();
        simpleAdmin.topicRunning();
    }
}
