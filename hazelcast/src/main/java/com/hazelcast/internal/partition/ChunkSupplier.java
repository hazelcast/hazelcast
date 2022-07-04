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

import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Iterator;
import java.util.function.BooleanSupplier;

/**
 * An iterator over collection of {@link Operation} which has
 * ability to be signaled to indicate end of iteration.
 *
 * @see ChunkedMigrationAwareService
 * @see com.hazelcast.spi.properties.ClusterProperty#PARTITION_CHUNKED_MIGRATION_ENABLED
 * @see com.hazelcast.spi.properties.ClusterProperty#PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB
 */
public interface ChunkSupplier extends Iterator<Operation> {

    /**
     * @param isEndOfChunk boolean supplier to signal end of chunk.
     */
    default void signalEndOfChunkWith(BooleanSupplier isEndOfChunk) {

    }
}
