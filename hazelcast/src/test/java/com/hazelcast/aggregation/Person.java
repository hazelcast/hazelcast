package com.hazelcast.aggregation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Person implements DataSerializable {

    public double age;

    public Person() {
    }

    public Person(double age) {
        this.age = age;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeDouble(age);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        age = in.readDouble();
    }
}
