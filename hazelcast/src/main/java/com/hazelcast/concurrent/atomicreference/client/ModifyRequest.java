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

package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;

import java.io.IOException;
import java.security.Permission;

import static com.hazelcast.nio.IOUtil.readNullableData;
import static com.hazelcast.nio.IOUtil.writeNullableData;

public abstract class ModifyRequest extends PartitionClientRequest implements Portable, SecureRequest {

    String name;
    Data update;

    public ModifyRequest() {
    }

    public ModifyRequest(String name, Data update) {
        this.name = name;
        this.update = update;
    }

    @Override
    protected int getPartition() {
        ClientEngine clientEngine = getClientEngine();
        Data key = serializationService.toData(name);
        return clientEngine.getPartitionService().getPartitionId(key);
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicReferencePortableHook.F_ID;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        ObjectDataOutput out = writer.getRawDataOutput();
        writeNullableData(out, update);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ObjectDataInput in = reader.getRawDataInput();
        update = readNullableData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
