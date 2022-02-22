/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.crdt;

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Base class for CRDT replication operations. It supports replicating a
 * map of {@link IdentifiedDataSerializable} values. In the case of CRDT
 * replication, this will be a map from map name to CRDT state.
 * Each concrete implementation of this class should be responsible for a
 * single CRDT type and the replication map should only contain CRDT
 * states for this CRDT type.
 *
 * @param <T> the CRDT type
 */
public abstract class AbstractCRDTReplicationOperation<T extends IdentifiedDataSerializable>
        extends Operation implements IdentifiedDataSerializable, MigrationCycleOperation {
    /** The map from CRDT name to CRDT state */
    private Map<String, T> replicationData;

    protected AbstractCRDTReplicationOperation() {
    }

    /**
     * Constructs the replication operation.
     *
     * @param replicationData the map of CRDT states to replicate
     */
    public AbstractCRDTReplicationOperation(Map<String, T> replicationData) {
        this.replicationData = replicationData;
    }

    @Override
    public void run() throws Exception {
        final CRDTReplicationAwareService<T> service = getService();
        for (Map.Entry<String, T> entry : replicationData.entrySet()) {
            service.merge(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        final CRDTReplicationMigrationService replicationMigrationService =
                getNodeEngine().getService(CRDTReplicationMigrationService.SERVICE_NAME);
        replicationMigrationService.scheduleMigrationTask(0);
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(replicationData.size());
        for (Entry<String, T> entry : replicationData.entrySet()) {
            out.writeString(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        final int mapSize = in.readInt();
        replicationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            final String name = in.readString();
            final T crdt = in.readObject();
            replicationData.put(name, crdt);
        }
    }
}
