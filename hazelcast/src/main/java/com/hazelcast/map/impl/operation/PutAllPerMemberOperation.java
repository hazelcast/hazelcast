/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Inserts the {@link MapEntries} for all partitions of a member via locally invoked {@link PutAllOperation}.
 * <p/>
 * Used to reduce the number of remote invocations of an {@link com.hazelcast.core.IMap#putAll(Map)} call.
 */
public class PutAllPerMemberOperation extends AbstractMultiPartitionOperation implements IdentifiedDataSerializable {

    private MapEntries[] mapEntries;

    @SuppressWarnings("unused")
    public PutAllPerMemberOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public PutAllPerMemberOperation(String name, int[] partitions, MapEntries[] mapEntries) {
        super(name, partitions);
        this.mapEntries = mapEntries;
    }

    @Override
    protected void doRun(int[] partitions, Future[] futures) {
        NodeEngine nodeEngine = getNodeEngine();
        for (int i = 0; i < partitions.length; i++) {
            Operation op = new PutAllOperation(name, mapEntries[i]);
            op.setNodeEngine(nodeEngine)
                    .setPartitionId(partitions[i])
                    .setReplicaIndex(getReplicaIndex())
                    .setService(getService())
                    .setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(op, getCallerAddress());
            futures[i] = nodeEngine.getOperationService().invokeOnPartition(op);
        }
    }

    @Override
    public Operation createFailureOperation(int failedPartitionId, int partitionIndex) {
        return new PutAllOperation(name, mapEntries[partitionIndex]);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        for (MapEntries entry : mapEntries) {
            entry.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = new MapEntries[getPartitionCount()];
        for (int i = 0; i < mapEntries.length; i++) {
            MapEntries entry = new MapEntries();
            entry.readData(in);
            mapEntries[i] = entry;
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_ALL_PER_MEMBER;
    }
}
