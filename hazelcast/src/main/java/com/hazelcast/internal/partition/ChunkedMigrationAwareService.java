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

import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Contract to be used to lazily migrate data in chunks.
 * <p>
 * Wins are more controlled heap usage and less pressure over network.
 * <p>
 * These wins come with potential trade-off of longer migration times.
 *
 * @see ChunkSupplier
 * @see ChunkSuppliers
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
