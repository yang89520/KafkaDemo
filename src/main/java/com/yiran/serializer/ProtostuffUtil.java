package com.yiran.serializer;

//import com.dyuproject.protostuff.LinkedBuffer;
//import com.dyuproject.protostuff.ProtostuffIOUtil;
//import com.dyuproject.protostuff.Schema;
//import com.dyuproject.protostuff.runtime.RuntimeSchema;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
///**
// * @program: BigdataSrc
// * @description: Protosuff序列化
// * protostuff 序列化/反序列化工具类
// * @author: Mr.Yang
// * @create: 2021-05-17 00:08
// **/
//
//
//public class ProtostuffUtil {
//    private static Map<Class<?>, Schema<?>> cachedSchema = new ConcurrentHashMap<>();
//
//    /**
//     * 序列化
//     *
//     * @param message 序列化数据
//     * @param tClass  .class
//     * @param <T>     类型
//     * @return byte[]
//     */
//    public static <T> byte[] serializer(T message, Class<T> tClass) {
//        Schema<T> schema = getSchema(tClass);
//        return ProtostuffIOUtil.toByteArray(message, schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
//    }
//
//    /**
//     * 反序列化
//     *
//     * @param bytes  bytes
//     * @param tClass .class
//     * @param <T>    类型
//     * @return T
//     */
//    public static <T> T deserializer(byte[] bytes, Class<T> tClass) {
//        Schema<T> schema = getSchema(tClass);
//        T message = schema.newMessage();
//        ProtostuffIOUtil.mergeFrom(bytes, message, schema);
//        return message;
//    }
//
//    private static <T> Schema<T> getSchema(Class<T> tClass) {
//        Schema<T> schema = (Schema<T>) cachedSchema.get(tClass);
//        if (schema == null) {
//            schema = RuntimeSchema.createFrom(tClass);
//            cachedSchema.put(tClass, schema);
//        }
//        return schema;
//    }
//}
