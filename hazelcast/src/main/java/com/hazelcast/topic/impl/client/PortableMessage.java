/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic.impl.client;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.topic.impl.TopicPortableHook;

import java.io.IOException;

public class PortableMessage implements Portable {

    private Data message;
    private long publishTime;
    private String uuid;

    public PortableMessage() {
    }

    public PortableMessage(Data message, long publishTime, String uuid) {
        this.message = message;
        this.publishTime = publishTime;
        this.uuid = uuid;
    }

    public Data getMessage() {
        return message;
    }

    public long getPublishTime() {
        return publishTime;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicPortableHook.PORTABLE_MESSAGE;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("pt", publishTime);
        writer.writeUTF("u", uuid);
        message.writeData(writer.getRawDataOutput());
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        publishTime = reader.readLong("pt");
        uuid = reader.readUTF("u");
        message = new Data();
        message.readData(reader.getRawDataInput());
    }
}
