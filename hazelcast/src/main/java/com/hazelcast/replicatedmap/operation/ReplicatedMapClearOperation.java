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

package com.hazelcast.replicatedmap.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

public class ReplicatedMapClearOperation
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private String mapName;
    private boolean emptyReplicationQueue;

    public ReplicatedMapClearOperation() {
    }

    public ReplicatedMapClearOperation(String mapName, boolean emptyReplicationQueue) {
        this.mapName = mapName;
        this.emptyReplicationQueue = emptyReplicationQueue;
    }

    @Override
    public void run()
            throws Exception {

        ReplicatedMapService service = getService();
        ReplicatedRecordStore recordStore = service.getReplicatedRecordStore(mapName, false);
        if (recordStore != null) {
            recordStore.clear(false, emptyReplicationQueue);
        }
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.OP_CLEAR;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {

        super.writeInternal(out);
        out.writeUTF(mapName);
        out.writeBoolean(emptyReplicationQueue);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {

        super.readInternal(in);
        mapName = in.readUTF();
        emptyReplicationQueue = in.readBoolean();
    }
}
