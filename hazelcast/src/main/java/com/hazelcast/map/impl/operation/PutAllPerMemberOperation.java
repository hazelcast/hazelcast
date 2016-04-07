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

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;

import java.io.IOException;
import java.util.concurrent.Future;

public class PutAllPerMemberOperation extends MapOperation implements DataSerializable {

    private int[] partitions;
    private MapEntries[] mapEntries;

    public PutAllPerMemberOperation() {
    }

    public PutAllPerMemberOperation(String name, int[] partitions, MapEntries[] mapEntries) {
        super(name);
        this.partitions = partitions;
        this.mapEntries = mapEntries;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        Future[] futures = new Future[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            Operation op = new PutAllOperation(name, mapEntries[i]);
            op.setNodeEngine(nodeEngine)
                    .setPartitionId(partitions[i])
                    .setReplicaIndex(getReplicaIndex())
                    .setServiceName(getServiceName())
                    .setService(getService())
                    .setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(op, getCallerAddress());
            futures[i] = nodeEngine.getOperationService().invokeOnPartition(op);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeIntArray(partitions);
        for (MapEntries entry : mapEntries) {
            entry.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        partitions = in.readIntArray();
        mapEntries = new MapEntries[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            MapEntries entry = new MapEntries();
            entry.readData(in);
            mapEntries[i] = entry;
        }
    }
}
