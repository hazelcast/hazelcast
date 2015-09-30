/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client request class for {@link com.hazelcast.core.ReplicatedMap#put(Object, Object, long, java.util.concurrent.TimeUnit)}
 * implementation
 */
public class ClientReplicatedMapPutTtlRequest extends PartitionClientRequest {

    private String name;
    private Data key;
    private Data value;
    private long ttlMillis;

    public ClientReplicatedMapPutTtlRequest() {
    }

    public ClientReplicatedMapPutTtlRequest(String name, Data key, Data value, long ttlMillis) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("ttlMillis", ttlMillis);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ttlMillis = reader.readLong("ttlMillis");
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
        value = in.readData();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.PUT_TTL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "put";
    }

    @Override
    public Object[] getParameters() {
        if (ttlMillis == 0) {
            return new Object[]{key, value};
        }
        return new Object[]{key, value, ttlMillis, TimeUnit.MILLISECONDS};
    }

    @Override
    protected Operation prepareOperation() {
        return new PutOperation(name, key, value, ttlMillis);
    }

    @Override
    protected int getPartition() {
        InternalPartitionService partitionService = clientEngine.getPartitionService();
        return partitionService.getPartitionId(key);
    }
}
