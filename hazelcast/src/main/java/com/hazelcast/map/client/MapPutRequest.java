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

package com.hazelcast.map.client;

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.PutOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class MapPutRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    protected Data key;
    protected Data value;
    protected String name;
    protected long threadId;
    protected long ttl;
    protected transient long startTime;
    protected boolean async;

    public MapPutRequest() {
    }

    public MapPutRequest(String name, Data key, Data value, long threadId, long ttl) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.threadId = threadId;
        this.ttl = ttl;
    }

    public MapPutRequest(String name, Data key, Data value, long threadId) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.threadId = threadId;
        this.ttl = -1;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.PUT;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected void beforeProcess() {
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void beforeResponse() {
        final long latency = System.currentTimeMillis() - startTime;
        final MapService mapService = getService();
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(name);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapService.getMapServiceContext().getLocalMapStatsProvider()
                    .getLocalMapStatsImpl(name).incrementPuts(latency);
        }
    }

    @Override
    protected Operation prepareOperation() {
        PutOperation op = new PutOperation(name, key, value, ttl);
        op.setThreadId(threadId);
        return op;
    }

    public void setAsAsync() {
        this.async = true;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("t", threadId);
        writer.writeLong("ttl", ttl);
        writer.writeBoolean("a", async);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        value.writeData(out);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        threadId = reader.readLong("t");
        ttl = reader.readLong("ttl");
        async = reader.readBoolean("a");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        value = new Data();
        value.readData(in);
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        if (async) {
            return "putAsync";
        }
        return "put";
    }

    @Override
    public Object[] getParameters() {
        if (ttl == -1) {
            return new Object[]{key, value};
        }
        return new Object[]{key, value, ttl, TimeUnit.MILLISECONDS};
    }
}
