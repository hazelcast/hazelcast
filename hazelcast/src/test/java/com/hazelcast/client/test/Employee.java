/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */

package com.hazelcast.client.test;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class Employee implements Portable {

    public static final int CLASS_ID = 2;
    private String name;
    private int age;

    // add all possible types
    byte by = 2;
    boolean bool = true;
    char c = 'c';
    short s = 4;
    int i = 2000;
    long l = 321324141;
    float f = 3.14f;
    double d = 3.14334;
    String str = "Hello world";
    String utfStr = "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム";

    byte[] byteArray = {50, 100, (byte) 150, (byte) 200};
    char[] charArray = {'c', 'h', 'a', 'r'};
    boolean[] boolArray = {true, false, false, true};
    short[] shortArray = {3, 4, 5};
    int[] integerArray = {9, 8, 7, 6};
    long[] longArray = {0, 1, 5, 7, 9, 11};
    float[] floatArray = {0.6543f, -3.56f, 45.67f};
    double[] doubleArray = {456.456, 789.789, 321.321};

    public Employee() {
    }

    public Employee(int age, String name) {
        this.age = age;
        this.name = name;
    }

    public int getFactoryId() {
        return PortableFactory.FACTORY_ID;
    }

    public int getClassId() {
        return CLASS_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeString("n", name);
        writer.writeInt("a", age);

        writer.writeByte("b", by);
        writer.writeChar("c", c);
        writer.writeBoolean("bo", bool);
        writer.writeShort("s", s);
        writer.writeInt("i", i);
        writer.writeLong("l", l);
        writer.writeFloat("f", f);
        writer.writeDouble("d", d);
        writer.writeString("str", str);
        writer.writeString("utfstr", utfStr);

        writer.writeByteArray("bb", byteArray);
        writer.writeCharArray("cc", charArray);
        writer.writeBooleanArray("ba", boolArray);
        writer.writeShortArray("ss", shortArray);
        writer.writeIntArray("ii", integerArray);
        writer.writeFloatArray("ff", floatArray);
        writer.writeDoubleArray("dd", doubleArray);

        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(by);
        out.writeObject(c);
        out.writeObject(bool);
        out.writeObject(s);
        out.writeObject(i);
        out.writeObject(f);
        out.writeObject(d);
        out.writeObject(str);
        out.writeObject(utfStr);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readString("n");
        age = reader.readInt("a");

        by = reader.readByte("b");
        c = reader.readChar("c");
        bool = reader.readBoolean("bo");
        s = reader.readShort("s");
        i = reader.readInt("i");
        l = reader.readLong("l");
        f = reader.readFloat("f");
        d = reader.readDouble("d");
        str = reader.readString("str");
        utfStr = reader.readString("utfstr");

        byteArray = reader.readByteArray("bb");
        charArray = reader.readCharArray("cc");
        boolArray = reader.readBooleanArray("ba");
        shortArray = reader.readShortArray("ss");
        integerArray = reader.readIntArray("ii");
        floatArray = reader.readFloatArray("ff");
        doubleArray = reader.readDoubleArray("dd");

        ObjectDataInput in = reader.getRawDataInput();
        by = (Byte) in.readObject();
        c = (Character) in.readObject();
        bool = (Boolean) in.readObject();
        s = (Short) in.readObject();
        i = (Integer) in.readObject();
        f = (Float) in.readObject();
        d = (Double) in.readObject();
        str = (String) in.readObject();
        utfStr = (String) in.readObject();
    }

    public int getAge() {
        return age;
    }
}
