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
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Client request class for {@link java.util.Map#get(Object)} implementation
 */
public class ClientReplicatedMapGetRequest
        extends AbstractReplicatedMapClientRequest {

    private Object key;

    ClientReplicatedMapGetRequest() {
        super(null);
    }

    public ClientReplicatedMapGetRequest(String mapName, Object key) {
        super(mapName);
        this.key = key;
    }

    @Override
    public Object call()
            throws Exception {

        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        ReplicatedRecord record = recordStore.getReplicatedRecord(key);

        Object value = null;
        long ttl = 0;
        long updateTime = 0;
        if (record != null) {
            value = recordStore.unmarshallValue(record.getValue());
            ttl = record.getTtlMillis();
            updateTime = record.getUpdateTime();
        }
        return new ReplicatedMapGetResponse(value, ttl, updateTime);
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {

        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {

        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.GET;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "get";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }
}
