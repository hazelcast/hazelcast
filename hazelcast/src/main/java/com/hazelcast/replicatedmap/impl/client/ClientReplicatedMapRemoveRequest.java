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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.impl.operation.RemoveOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;

/**
 * Client request class for {@link java.util.Map#remove(Object)} implementation
 */
public class ClientReplicatedMapRemoveRequest extends AbstractReplicatedMapClientRequest {

    private Data key;

    public ClientReplicatedMapRemoveRequest() {
    }

    public ClientReplicatedMapRemoveRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    @Override
    protected Operation prepareOperation() {
        return new RemoveOperation(getMapName(), key);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    @Override
    protected Object filter(Object response) {
        if (response instanceof VersionResponsePair) {
            VersionResponsePair pair = (VersionResponsePair) response;
            return pair.getResponse();
        }
        return response;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.REMOVE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }

    @Override
    protected int getPartition() {
        return getClientEngine().getPartitionService().getPartitionId(key);
    }
}
