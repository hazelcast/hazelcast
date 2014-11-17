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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation;
import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

/**
 * This client request  specifically calls {@link CacheKeyIteratorOperation} on the server side.
 *
 * @see com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation
 */
public class CacheIterateRequest
        extends PartitionClientRequest
        implements RetryableRequest {

    private String name;
    private int partitionId;
    private int tableIndex;
    private int batch;
    private InMemoryFormat inMemoryFormat;

    public CacheIterateRequest() {
    }

    public CacheIterateRequest(String name, int partitionId, int tableIndex, int batch, InMemoryFormat inMemoryFormat) {
        this.name = name;
        this.partitionId = partitionId;
        this.tableIndex = tableIndex;
        this.batch = batch;
        this.inMemoryFormat = inMemoryFormat;
    }

    @Override
    protected Operation prepareOperation() {
        ICacheService service = getService();
        CacheOperationProvider cacheOperationProvider = service.getCacheOperationProvider(name, inMemoryFormat);
        return cacheOperationProvider.createKeyIteratorOperation(tableIndex, batch);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ITERATE;
    }

    public final String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeInt("p", partitionId);
        writer.writeInt("t", tableIndex);
        writer.writeInt("b", batch);
        writer.writeUTF("i", inMemoryFormat.name());
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        partitionId = reader.readInt("p");
        tableIndex = reader.readInt("t");
        batch = reader.readInt("b");
        inMemoryFormat = InMemoryFormat.valueOf(reader.readUTF("i"));
    }
}
