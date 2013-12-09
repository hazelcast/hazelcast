package com.hazelcast.replicatedmap.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStorage;
import com.hazelcast.spi.Operation;

import java.io.IOException;

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

public class ReplicatedMapPostJoinOperation
        extends AbstractReplicatedMapOperation
        implements IdentifiedDataSerializable {

    private String[] replicatedMaps;
    private int chunkSize;

    public ReplicatedMapPostJoinOperation() {
    }

    public ReplicatedMapPostJoinOperation(String[] replicatedMaps, int chunkSize) {
        this.replicatedMaps = replicatedMaps;
        this.chunkSize = chunkSize;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService replicatedMapService = getService();
        for (String replicatedMap : replicatedMaps) {
            AbstractReplicatedRecordStorage replicatedRecordStorage =
                    (AbstractReplicatedRecordStorage) replicatedMapService.getReplicatedRecordStore(replicatedMap);

            replicatedRecordStorage.queueInitialFillup(getCallerAddress(), chunkSize);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(chunkSize);
        out.writeInt(replicatedMaps.length);
        for (int i = 0; i < replicatedMaps.length; i++) {
            out.writeUTF(replicatedMaps[i]);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        chunkSize = in.readInt();
        int length = in.readInt();
        replicatedMaps = new String[length];
        for (int i = 0; i < length; i++) {
            replicatedMaps[i] = in.readUTF();
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.OP_POST_JOIN;
    }

}
