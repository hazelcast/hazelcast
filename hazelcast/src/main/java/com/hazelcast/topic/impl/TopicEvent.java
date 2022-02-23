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

package com.hazelcast.topic.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;

class TopicEvent implements IdentifiedDataSerializable {

    String name;
    long publishTime;
    Address publisherAddress;
    Data data;

    TopicEvent() {
    }

    TopicEvent(String name, Data data, Address publisherAddress) {
        this.name = name;
        this.publishTime = Clock.currentTimeMillis();
        this.publisherAddress = publisherAddress;
        this.data = data;
    }

    @Override
    public int getFactoryId() {
        return TopicDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicDataSerializerHook.TOPIC_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(publishTime);
        out.writeObject(publisherAddress);
        IOUtil.writeData(out, data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        publishTime = in.readLong();
        publisherAddress = in.readObject();
        data = IOUtil.readData(in);
    }

    @Override
    public String toString() {
        return "TopicEvent{"
                + "name='" + name + '\''
                + ", publishTime=" + publishTime
                + ", publisherAddress=" + publisherAddress
                + '}';
    }
}
