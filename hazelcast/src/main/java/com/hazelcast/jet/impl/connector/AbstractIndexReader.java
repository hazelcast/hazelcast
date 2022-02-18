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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Stateless interface to read the map index
 *
 * @param <B> type of the batch object
 * @param <R> type of the record
 */
public abstract class AbstractIndexReader<B, R> {
    protected String objectName;
    protected InternalSerializationService serializationService;

    protected FunctionEx<B, List<R>> toRecordSetFn;

    public AbstractIndexReader(@Nonnull String objectName, @Nonnull FunctionEx<B, List<R>> toRecordSetFn) {
        this.objectName = objectName;
        this.toRecordSetFn = toRecordSetFn;
    }

    @Nonnull
    public abstract CompletableFuture<MapFetchIndexOperationResult> readBatch(
            Address address,
            PartitionIdSet partitions,
            IndexIterationPointer[] pointers
    );

    @SuppressWarnings("unchecked")
    @Nonnull
    public B toBatchResult(@Nonnull CompletableFuture<MapFetchIndexOperationResult> future)
            throws ExecutionException, InterruptedException {
        return (B) future.get();
    }

    @Nonnull
    public final List<R> toRecordSet(@Nonnull B result) {
        return toRecordSetFn.apply(result);
    }
}
