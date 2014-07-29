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

/**
 * Client request class for {@link java.util.Map#containsValue(Object)} implementation
 */
public class ClientReplicatedMapContainsValueRequest
        extends AbstractReplicatedMapClientRequest {

    private Object value;

    ClientReplicatedMapContainsValueRequest() {
        super(null);
    }

    public ClientReplicatedMapContainsValueRequest(String mapName, Object value) {
        super(mapName);
        this.value = value;
    }

    @Override
    public Object call()
            throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        return recordStore.containsValue(value);
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(value);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        value = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.CONTAINS_VALUE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "containsValue";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{value};
    }
}
