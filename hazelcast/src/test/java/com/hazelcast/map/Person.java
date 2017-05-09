package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Person implements DataSerializable {

    private int age;
    private String name;

    public Person() {
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(age);
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        age = in.readInt();
        name = in.readUTF();
    }

}

