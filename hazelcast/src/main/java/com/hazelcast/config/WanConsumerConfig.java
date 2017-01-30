/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Config to be used by WanReplicationConsumer instances (EE only).
 */
public class WanConsumerConfig implements DataSerializable {

    private Map<String, Comparable> properties = new HashMap<String, Comparable>();
    private String className;
    private Object implementation;

    public Map<String, Comparable> getProperties() {
        return properties;
    }

    public WanConsumerConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public WanConsumerConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Object getImplementation() {
        return implementation;
    }

    public WanConsumerConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public String toString() {
        return "WanConsumerConfig{"
                + "properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = properties.size();
        out.writeInt(size);
        for (Map.Entry<String, Comparable> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeUTF(className);
        out.writeObject(implementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), (Comparable) in.readObject());
        }
        className = in.readUTF();
        implementation = in.readObject();
    }
}
