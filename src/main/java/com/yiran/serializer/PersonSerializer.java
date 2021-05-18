package com.yiran.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;
/**
 * @program: BigdataSrc
 * @description:
 * @author: Mr.Yang
 * @create: 2021-05-16 22:54
 **/



/**
 * Kafka序列化器：
 * - configure(Map<String, ?> configs, boolean isKey)
 * - byte[] serialize(String s, Person person)
 * - byte[] serialize(String topic, Headers headers, Person data)
 * - void close()
 */
public class PersonSerializer implements Serializer<Person> {


    @Override
    public byte[] serialize(String s, Person person) {
        if (person != null) {
            return JSON.toJSONBytes(person);
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }

//    private List<String> beanInfo=new ArrayList<String>();
//    /**
//     * 获取Bean所有属性名称[和值] 返回给全局变量beanInfo数组
//     * 使用消耗性能暂时不用
//     * @param person
//     */
//    public void getBeanInfo(Person person){
//        BeanInfo bInfo;
//        try{
//            //throws IntrospectionException
//            bInfo= Introspector.getBeanInfo(Person.class, Object.class);
//            if(bInfo!=null){
//                PropertyDescriptor[] propertyDescriptors = bInfo.getPropertyDescriptors();
//                for(PropertyDescriptor p:propertyDescriptors){
//                    //获得属性名称
//                    System.err.println(p.getName());
//                    beanInfo.add(p.getName());
//                    //调用该属性名称对应的getter方法
//                    //throws IntrospectionException,InvocationTargetException,IllegalAccessException
//                    Object obj = new PropertyDescriptor(p.getName(), Person.class).getReadMethod().invoke(person);
//                    //调用该属性名称对应的setter方法
//                    //throws IntrospectionException,InvocationTargetException,IllegalAccessException
//                    new PropertyDescriptor(p.getName(), Person.class).getWriteMethod().invoke(person,new Object[]{"1"});
//                }
//            }
//        }catch (IntrospectionException e){
//            e.printStackTrace();
//        }catch (InvocationTargetException e1){
//            e1.printStackTrace();
//        }catch (IllegalAccessException e2){
//            e2.printStackTrace();
//        }
//    }
}
