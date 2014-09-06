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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client request class for {@link com.hazelcast.core.ReplicatedMap#put(Object, Object, long, java.util.concurrent.TimeUnit)}
 * implementation
 */
public class ClientReplicatedMapPutTtlRequest
        extends AbstractReplicatedMapClientRequest {

    private Object key;
    private Object value;
    private long ttlMillis;

    ClientReplicatedMapPutTtlRequest() {
        super(null);
    }

    public ClientReplicatedMapPutTtlRequest(String mapName, Object key, Object value, long ttlMillis) {
        super(mapName);
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public Object call()
            throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        return recordStore.put(key, value, ttlMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeLong("ttlMillis", ttlMillis);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
        out.writeObject(value);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        ttlMillis = reader.readLong("ttlMillis");
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
        value = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.PUT_TTL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_PUT);
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
}
