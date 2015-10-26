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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheOperationProvider;
import com.hazelcast.cache.impl.CachePortableHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.security.Permission;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This client request that puts entries as batch into specified partition.
 */
public class CachePutAllRequest
        extends PartitionClientRequest
        implements CompletionAwareCacheRequest, RetryableRequest {

    private String name;
    private InMemoryFormat inMemoryFormat;
    private int partitionId;
    private int completionId;
    private List<Map.Entry<Data, Data>> entries;
    private ExpiryPolicy expiryPolicy;

    public CachePutAllRequest() {
    }

    public CachePutAllRequest(String name, InMemoryFormat inMemoryFormat,
                              List<Map.Entry<Data, Data>> entries, ExpiryPolicy expiryPolicy, int partitionId) {
        this.name = name;
        this.inMemoryFormat = inMemoryFormat;
        this.entries = entries;
        this.expiryPolicy = expiryPolicy;
        this.partitionId = partitionId;
    }

    @Override
    protected Operation prepareOperation() {
        ICacheService service = getService();
        CacheOperationProvider operationProvider = service.getCacheOperationProvider(name, inMemoryFormat);
        return operationProvider.createPutAllOperation(entries, expiryPolicy, completionId);
    }

    @Override
    public void setCompletionId(Integer completionId) {
        this.completionId = completionId != null ? completionId : -1;
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.PUT_ALL;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeUTF("i", inMemoryFormat.name());
        writer.writeInt("p", partitionId);
        writer.writeInt("c", completionId);
        writer.writeInt("s", entries.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Map.Entry<Data, Data> entry : entries) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
        out.writeObject(expiryPolicy);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        inMemoryFormat = InMemoryFormat.valueOf(reader.readUTF("i"));
        partitionId = reader.readInt("p");
        completionId = reader.readInt("c");
        int size = reader.readInt("s");
        ObjectDataInput in = reader.getRawDataInput();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
        expiryPolicy = in.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public Object[] getParameters() {
        if (expiryPolicy == null) {
            return new Object[]{entries};
        }
        return new Object[]{entries, expiryPolicy};
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

}
