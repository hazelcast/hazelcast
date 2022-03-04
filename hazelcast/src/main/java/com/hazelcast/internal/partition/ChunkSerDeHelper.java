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

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.BooleanSupplier;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static java.lang.String.format;

/**
 * Helper class for serialization and deserialization of chunks.
 *
 * @see ChunkedMigrationAwareService
 */
public final class ChunkSerDeHelper {

    private final ILogger logger;
    private final int partitionId;
    private final Collection<ChunkSupplier> chunkSuppliers;
    private final int maxTotalChunkedDataInBytes;

    public ChunkSerDeHelper(ILogger logger, int partitionId,
                            Collection<ChunkSupplier> chunkSuppliers,
                            boolean chunkedMigrationEnabled,
                            int maxTotalChunkedDataInBytes) {
        assert chunkSuppliers != null;
        assert logger != null;
        assert !chunkedMigrationEnabled || (maxTotalChunkedDataInBytes > 0)
                : "Found maxTotalChunkedDataInBytes=" + maxTotalChunkedDataInBytes;

        this.logger = logger;
        this.partitionId = partitionId;
        this.chunkSuppliers = chunkSuppliers;
        this.maxTotalChunkedDataInBytes = maxTotalChunkedDataInBytes;
    }

    public static Collection<Operation> readChunkedOperations(ObjectDataInput in,
                                                              Collection<Operation> operations) throws IOException {
        do {
            Operation operation = in.readObject();
            if (operation == null) {
                break;
            }
            if (isEmpty(operations)) {
                operations = new LinkedList<>();
            }
            operations.add(operation);
        } while (true);

        return operations;
    }


    public void writeChunkedOperations(ObjectDataOutput out) throws IOException {
        IsEndOfChunk isEndOfChunk = new IsEndOfChunk(out, maxTotalChunkedDataInBytes);

        for (ChunkSupplier chunkSupplier : chunkSuppliers) {

            chunkSupplier.signalEndOfChunkWith(isEndOfChunk);

            while (chunkSupplier.hasNext()) {
                Operation chunk = chunkSupplier.next();
                // legacy migration operations which don't support
                // chunked migration can return null here. for
                // legacy migration operations we return existing
                // migration operation as if it is a single chunk.
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
            assert maxTotalChunkedDataInBytes > 0
                    : "Found maxTotalChunkedDataInBytes: " + maxTotalChunkedDataInBytes;

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
}
