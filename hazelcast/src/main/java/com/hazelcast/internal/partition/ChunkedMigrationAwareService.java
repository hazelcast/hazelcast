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

import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Contract to be used to migrate data of a partition in chunks.
 * <p>
 * Depending on their implementation, chunks can be created
 * lazily. By the help of chunks, large partition data can be
 * transferred to a target destination by keeping heap memory
 * under control and by having less pressure over network.
 * <p>
 * If a partition data doesn't fit into one chunk, increased migration times can be observed.
 *
 * @see ChunkSupplier
 * @see ChunkSuppliers
 * @see com.hazelcast.spi.properties.ClusterProperty#PARTITION_CHUNKED_MIGRATION_ENABLED
 * @see com.hazelcast.spi.properties.ClusterProperty#PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB
 */
public interface ChunkedMigrationAwareService
        extends FragmentedMigrationAwareService {

    /**
     * By default, a {@link ChunkedMigrationAwareService} behaves
     * identical with a {@link FragmentedMigrationAwareService}.
     * <p>
     * In other words, a chunk equals a fragment. To divide
     * a fragment further, services can implement their own
     * {@link ChunkSupplier} by implementing this method.
     *
     * @param event      partition replication event
     * @param namespaces collection of namespaces
     * @return a new {@link ChunkSupplier} object.
     */
    default ChunkSupplier newChunkSupplier(PartitionReplicationEvent event,
                                           Collection<ServiceNamespace> namespaces) {
        return ChunkSuppliers.newSingleChunkSupplier(
                new Supplier<Operation>() {
                    @Override
                    public Operation get() {
                        return ChunkedMigrationAwareService.this.prepareReplicationOperation(event, namespaces);
                    }

                    @Override
                    public String toString() {
                        return format("OperationSupplier{service: %s}",
                                ChunkedMigrationAwareService.this.getClass().getSimpleName());
                    }
                });
    }
}
