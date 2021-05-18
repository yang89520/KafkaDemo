package com.yiran.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * @program: BigdataSrc
 * @description:反序列化器
 * @author: Mr.Yang
 * @create: 2021-05-16 22:57
 **/

public class PersonDeserializer implements Deserializer<Person> {
    private String encoding="UTF8";
    @Override
    public Person deserialize(String s, byte[] bytes) {
        if (bytes != null) {
            return JSON.parseObject(bytes,Person.class);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
