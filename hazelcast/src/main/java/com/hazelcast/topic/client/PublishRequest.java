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

package com.hazelcast.topic.client;

import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;
import com.hazelcast.topic.PublishOperation;
import com.hazelcast.topic.TopicPortableHook;
import com.hazelcast.topic.TopicService;

import java.io.IOException;

/**
 * @author ali 5/14/13
 */
public class PublishRequest extends PartitionClientRequest implements Portable, InitializingObjectRequest {

    String name;

    private Data message;

    public PublishRequest() {
    }

    public PublishRequest(String name, Data message) {
        this.name = name;
        this.message = message;
    }

    protected Operation prepareOperation() {
        return new PublishOperation(name, message);
    }

    protected int getPartition() {
        Data key = getClientEngine().toData(name);
        return getClientEngine().getPartitionService().getPartitionId(key);
    }

    protected int getReplicaIndex() {
        return 0;
    }

    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    public int getClassId() {
        return TopicPortableHook.PUBLISH;
    }

    public Object getObjectId() {
        return name;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        message.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        message = new Data();
        message.readData(in);
    }
}
