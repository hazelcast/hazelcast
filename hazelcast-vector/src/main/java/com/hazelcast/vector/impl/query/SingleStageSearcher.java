/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.query;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.ops.SearchOperationsFactory;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.vector.impl.VectorCollectionService.SERVICE_NAME;

/**
 * Single stage aggregation. All partitions are queried individually and partition results are aggregated
 * by the coordinator.
 */
public class SingleStageSearcher implements Searcher {

    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 2L * REFERENCE_COST_IN_BYTES;

    private final IPartitionService partitionService;
    private final OperationService operationService;

    public SingleStageSearcher(NodeEngine engine) {
        partitionService = engine.getPartitionService();
        operationService = engine.getOperationService();
    }

    @Override
    public CompletableFuture<SearchResults<Data, Data>> search(String collectionName, VectorValues vectors, SearchOptions options,
                                                         @Nullable ClientEndpoint endpoint) {
        var searchFactory = new SearchOperationsFactory(collectionName, vectors, options);
        OperationFactory operationFactory = endpoint != null
                ? new OperationFactoryWrapper(searchFactory, endpoint.getUuid())
                : searchFactory;
        // Note that unlike TwoStageSearcher, here individual partition search (SearchOperation)
        // is started on partition thread and then offloads. This is extra thread switch, but
        // we can use generic invokeOnAllPartitionsAsync, and it does not matter if the operation
        // is local or remote. SearchOperation also executes in the same way as when TwoStageSearcher
        // retries the partitions. However, in principle, it could be optimized to avoid
        // operation going through partition thread, at least for local invocation.
        return operationService.invokeOnAllPartitionsAsync(SERVICE_NAME, operationFactory)
                .thenApplyAsync(map -> QueryResult.reduce(map, options, partitionService.getPartitionCount()), CALLER_RUNS);
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED;
    }
}
