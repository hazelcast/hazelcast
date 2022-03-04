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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.TargetAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.partition.ChunkSerDeHelper.readChunkedOperations;

/**
 * Contains fragment namespaces along with their
 * partition versions and migration data operations
 *
 * @since 3.9
 */
public class ReplicaFragmentMigrationState
        implements IdentifiedDataSerializable, TargetAware, Versioned {

    private Map<ServiceNamespace, long[]> namespaces;
    private Collection<Operation> migrationOperations;

    private transient ChunkSerDeHelper chunkSerDeHelper;

    public ReplicaFragmentMigrationState() {
    }

    public ReplicaFragmentMigrationState(Map<ServiceNamespace, long[]> namespaces,
                                         Collection<Operation> migrationOperations,
                                         Collection<ChunkSupplier> chunkSuppliers,
                                         boolean chunkedMigrationEnabled,
                                         int maxTotalChunkedDataInBytes, ILogger logger,
                                         int partitionId) {
        this.namespaces = namespaces;
        this.migrationOperations = migrationOperations;
        this.chunkSerDeHelper = new ChunkSerDeHelper(logger, partitionId,
                chunkSuppliers, chunkedMigrationEnabled, maxTotalChunkedDataInBytes);
    }

    public Map<ServiceNamespace, long[]> getNamespaceVersionMap() {
        return namespaces;
    }

    public Collection<Operation> getMigrationOperations() {
        return migrationOperations;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_FRAGMENT_MIGRATION_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(namespaces.size());
        for (Map.Entry<ServiceNamespace, long[]> e : namespaces.entrySet()) {
            out.writeObject(e.getKey());
            out.writeLongArray(e.getValue());
        }

        out.writeInt(migrationOperations.size());
        for (Operation operation : migrationOperations) {
            out.writeObject(operation);
        }

        chunkSerDeHelper.writeChunkedOperations(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int namespaceSize = in.readInt();
        namespaces = new HashMap<>(namespaceSize);
        for (int i = 0; i < namespaceSize; i++) {
            ServiceNamespace namespace = in.readObject();
            long[] replicaVersions = in.readLongArray();
            namespaces.put(namespace, replicaVersions);
        }
        int migrationOperationSize = in.readInt();
        migrationOperations = new ArrayList<>(migrationOperationSize);
        for (int i = 0; i < migrationOperationSize; i++) {
            Operation migrationOperation = in.readObject();
            migrationOperations.add(migrationOperation);
        }

        migrationOperations = readChunkedOperations(in, migrationOperations);
    }

    @Override
    public void setTarget(Address address) {
        for (Operation op : migrationOperations) {
            if (op instanceof TargetAware) {
                ((TargetAware) op).setTarget(address);
            }
        }
    }
}
