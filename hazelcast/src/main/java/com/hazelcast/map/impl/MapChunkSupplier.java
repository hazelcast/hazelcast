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

package com.hazelcast.map.impl;

import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.operation.MapChunk;
import com.hazelcast.map.impl.operation.MapChunkContext;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.function.BooleanSupplier;

/**
 * Once instance created per record-store during migration.
 */
class MapChunkSupplier implements ChunkSupplier {

    protected final MapChunkContext context;

    protected BooleanSupplier isEndOfChunk;

    private final int partitionId;
    private final int replicaIndex;
    private final MapServiceContext mapServiceContext;

    // written by 1 but read by multiple threads
    private volatile int chunkNumber;

    MapChunkSupplier(MapServiceContext mapServiceContext, ServiceNamespace namespace,
                     int partitionId, int replicaIndex) {
        this.mapServiceContext = mapServiceContext;
        this.replicaIndex = replicaIndex;
        this.partitionId = partitionId;
        this.context = createMapChunkContext(mapServiceContext, namespace, partitionId);
    }

    // overridden in EE
    protected MapChunkContext createMapChunkContext(MapServiceContext mapServiceContext,
                                                    ServiceNamespace namespace, int partitionId) {
        return new MapChunkContext(mapServiceContext, partitionId, namespace);
    }

    // overridden in EE
    protected Operation createChunkOperation(int chunkNumber) {
        return new MapChunk(context, chunkNumber, isEndOfChunk);
    }

    @Override
    public final void signalEndOfChunkWith(BooleanSupplier isEndOfChunk) {
        this.isEndOfChunk = isEndOfChunk;
    }

    @Override
    public final Operation next() {
        assert isEndOfChunk != null : "isEndOfChunk must be set before";

        chunkNumber++;
        return createChunkOperation(chunkNumber)
                .setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex)
                .setServiceName(MapService.SERVICE_NAME)
                .setNodeEngine(mapServiceContext.getNodeEngine());
    }

    @Override
    public final boolean hasNext() {
        if (chunkNumber == 0) {
            // First chunk must be sent regardless of map has data
            // because in first chunk we also migrate metadata.
            return true;
        }
        return context.hasMoreChunks();
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName()
                + '{'
                + "partitionId=" + partitionId
                + ", chunkNumber=" + chunkNumber
                + ", mapName=" + context.getMapName()
                + '}';
    }
}
