/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestObject implements DataSerializable {

    boolean bool;
    byte b;
    char ch;
    short s1;
    int i;
    long l;
    float f;
    double d;

    String s;

    List<String> list;
    Set<String> set;
    Map<Integer, String> map;

    public TestObject() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public TestObject(boolean bool, byte b, char ch, short s1, int i, long l, float f, double d,
                      String s, List<String> list, Set<String> set, Map<Integer, String> map) {
        this.bool = bool;
        this.b = b;
        this.ch = ch;
        this.s1 = s1;
        this.i = i;
        this.l = l;
        this.f = f;
        this.d = d;
        this.s = s;
        this.list = list;
        this.set = set;
        this.map = map;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(bool);
        out.writeByte(b);
        out.writeChar(ch);
        out.writeShort(s1);
        out.writeInt(i);
        out.writeLong(l);
        out.writeFloat(f);
        out.writeDouble(d);
        out.writeUTF(s);

        out.writeObject(list);
        out.writeObject(set);
        out.writeObject(map);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bool = in.readBoolean();
        b = in.readByte();
        ch = in.readChar();
        s1 = in.readShort();
        i = in.readInt();
        l = in.readLong();
        f = in.readFloat();
        d = in.readDouble();
        s = in.readUTF();

        list = in.readObject();
        set = in.readObject();
        map = in.readObject();
    }
}
