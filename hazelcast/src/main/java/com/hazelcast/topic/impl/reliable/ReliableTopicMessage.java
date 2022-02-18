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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;

import static com.hazelcast.topic.impl.TopicDataSerializerHook.F_ID;
import static com.hazelcast.topic.impl.TopicDataSerializerHook.RELIABLE_TOPIC_MESSAGE;

/**
 * The Object that is going to be stored in the Ringbuffer. It contains the actual message payload and some metadata.
 */
@BinaryInterface
public class ReliableTopicMessage implements IdentifiedDataSerializable {
    private long publishTime;
    private Address publisherAddress;
    private Data payload;

    public ReliableTopicMessage() {
    }

    public ReliableTopicMessage(Data payload, Address publisherAddress) {
        this.publishTime = Clock.currentTimeMillis();
        this.publisherAddress = publisherAddress;
        this.payload = payload;
    }

    public long getPublishTime() {
        return publishTime;
    }

    public Address getPublisherAddress() {
        return publisherAddress;
    }

    public Data getPayload() {
        return payload;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getClassId() {
        return RELIABLE_TOPIC_MESSAGE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(publishTime);
        out.writeObject(publisherAddress);
        IOUtil.writeData(out, payload);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        publishTime = in.readLong();
        publisherAddress = in.readObject();
        payload = IOUtil.readData(in);
    }
}
