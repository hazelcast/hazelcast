/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemorySize;
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
import java.util.function.BooleanSupplier;

import static java.lang.String.format;

/**
 * Contains fragment namespaces along with their partition versions and migration data operations
 *
 * @since 3.9
 */
public class ReplicaFragmentMigrationState
        implements IdentifiedDataSerializable, TargetAware, Versioned {

    private Map<ServiceNamespace, long[]> namespaces;
    private Collection<Operation> migrationOperations;

    private transient Collection<ChunkSupplier> chunkSuppliers;
    private transient int maxTotalChunkedDataInBytes;
    private transient ILogger logger;
    private transient int partitionId;

    public ReplicaFragmentMigrationState() {
    }

    public ReplicaFragmentMigrationState(Map<ServiceNamespace, long[]> namespaces,
                                         Collection<Operation> migrationOperations,
                                         Collection<ChunkSupplier> chunkSuppliers,
                                         int maxTotalChunkedDataInBytes, ILogger logger, int partitionId) {
        assert chunkSuppliers != null;
        assert logger != null;

        this.namespaces = namespaces;
        this.migrationOperations = migrationOperations;
        this.chunkSuppliers = chunkSuppliers;
        this.maxTotalChunkedDataInBytes = maxTotalChunkedDataInBytes;
        this.logger = logger;
        this.partitionId = partitionId;
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

        // RU_COMPAT 5.0
        if (out.getVersion().isGreaterOrEqual(Versions.V5_1)) {
            writeChunkedOperations(out);
        }
    }

    private void writeChunkedOperations(ObjectDataOutput out) throws IOException {
        IsEndOfChunk isEndOfChunk = new IsEndOfChunk(out, maxTotalChunkedDataInBytes);

        for (ChunkSupplier chunkSupplier : chunkSuppliers) {

            chunkSupplier.signalEndOfChunkWith(isEndOfChunk);

            while (chunkSupplier.hasNext()) {
                Operation chunk = chunkSupplier.next();
                if (chunk == null) {
                    break;
                }

                logCurrentChunk(chunkSupplier);

                out.writeObject(chunk);

                if (isEndOfChunk.getAsBoolean()) {
                    break;
                }
            }

            if (isEndOfChunk.getAsBoolean()) {
                logEndOfChunk(isEndOfChunk);
                break;
            }
        }
        // indicates end of chunked state
        out.writeObject(null);

        logEndOfAllChunks(isEndOfChunk);
    }

    private void logCurrentChunk(ChunkSupplier chunkSupplier) {
        if (!logger.isFinestEnabled()) {
            return;
        }

        logger.finest(String.format("Current chunk [partitionId:%d, %s]",
                partitionId, chunkSupplier));
    }

    private void logEndOfChunk(IsEndOfChunk isEndOfChunk) {
        if (!logger.isFinestEnabled()) {
            return;
        }

        logger.finest(format("Chunk is full [partitionId:%d, maxChunkSize:%s, actualChunkSize:%s]",
                partitionId,
                MemorySize.toPrettyString(maxTotalChunkedDataInBytes),
                MemorySize.toPrettyString(isEndOfChunk.bytesWrittenSoFar())));
    }

    private void logEndOfAllChunks(IsEndOfChunk isEndOfChunk) {
        if (!logger.isFinestEnabled()) {
            return;
        }

        boolean allDone = true;
        for (ChunkSupplier chunkSupplier : chunkSuppliers) {
            if (chunkSupplier.hasNext()) {
                allDone = false;
                break;
            }
        }

        if (allDone) {
            logger.finest(format("Last chunk was sent [partitionId:%d, maxChunkSize:%s, actualChunkSize:%s]",
                    partitionId,
                    MemorySize.toPrettyString(maxTotalChunkedDataInBytes),
                    MemorySize.toPrettyString(isEndOfChunk.bytesWrittenSoFar())));
        }
    }

    private static final class IsEndOfChunk implements BooleanSupplier {

        private final int positionStart;
        private final int maxTotalChunkedDataInBytes;
        private final BufferObjectDataOutput out;

        private IsEndOfChunk(ObjectDataOutput out, int maxTotalChunkedDataInBytes) {
            this.out = ((BufferObjectDataOutput) out);
            this.positionStart = ((BufferObjectDataOutput) out).position();
            this.maxTotalChunkedDataInBytes = maxTotalChunkedDataInBytes;
        }

        @Override
        public boolean getAsBoolean() {
            return bytesWrittenSoFar() >= maxTotalChunkedDataInBytes;
        }

        public int bytesWrittenSoFar() {
            return out.position() - positionStart;
        }
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

        // RU_COMPAT 5.0
        if (in.getVersion().isGreaterOrEqual(Versions.V5_1)) {
            readChunkedOperations(in);
        }
    }

    private void readChunkedOperations(ObjectDataInput in) throws IOException {
        do {
            Object operation = in.readObject();
            if (operation == null) {
                break;
            }
            migrationOperations.add(((Operation) operation));
        } while (true);
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
