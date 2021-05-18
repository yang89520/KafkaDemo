package com.yiran.serializer;

/**
 * @program: BigdataSrc
 * @description:
 * @author: Mr.Yang
 * @create: 2021-05-16 22:55
 **/

/**
 * 被序列化的bean
 */
public class Person{
    private String name;
    private String sex;
    private String age;

    public Person() {
    }
    public Person(String name, String sex, String age) {
        this.name = name;
        this.sex = sex;
        this.age = age;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getSex() {
        return sex;
    }
    public void setSex(String sex) {
        this.sex = sex;
    }
    public String getAge() {
        return age;
    }
    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                '}';
    }
}
