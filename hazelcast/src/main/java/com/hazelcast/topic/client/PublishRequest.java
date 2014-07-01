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

import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.topic.PublishOperation;
import com.hazelcast.topic.TopicPortableHook;
import com.hazelcast.topic.TopicService;

import java.io.IOException;
import java.security.Permission;

public class PublishRequest extends PartitionClientRequest implements Portable, SecureRequest {

    private String name;
    private Data message;

    public PublishRequest() {
    }

    public PublishRequest(String name, Data message) {
        this.name = name;
        this.message = message;
    }

    @Override
    protected Operation prepareOperation() {
        return new PublishOperation(name, message);
    }

    @Override
    protected int getPartition() {
        ClientEngine clientEngine = getClientEngine();
        Data key = serializationService.toData(name);
        return clientEngine.getPartitionService().getPartitionId(key);
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicPortableHook.PUBLISH;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        ObjectDataOutput out = writer.getRawDataOutput();
        message.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ObjectDataInput in = reader.getRawDataInput();
        message = new Data();
        message.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(name, ActionConstants.ACTION_PUBLISH);
    }

    @Override
    public String getMethodName() {
        return "publish";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{message};
    }
}
