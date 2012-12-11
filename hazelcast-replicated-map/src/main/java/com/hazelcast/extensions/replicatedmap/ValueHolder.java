/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.extensions.replicatedmap;

import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ValueHolder<V> implements DataSerializable{
    
    private V value;
    private Vector vector;

    public ValueHolder() {
    }

    public ValueHolder(V value, Vector vector) {
        this.value = value;
        this.vector = vector;
    }

    public void writeData(DataOutput dataOutput) throws IOException {
        SerializationHelper.writeObject(dataOutput, value);
        vector.writeData(dataOutput);
    }

    public void readData(DataInput dataInput) throws IOException {
        value = (V)SerializationHelper.readObject(dataInput);
        vector = new Vector();
        vector.readData(dataInput);
    }

    public V getValue() {
        return value;
    }

    public Vector getVector() {
        return vector;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "ValueHolder{" +
                "value=" + value +
                ", vector=" + vector +
                '}';
    }
}


