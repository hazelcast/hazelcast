/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Stateless interface to read a map/cache.
 *
 * @param <F> type of the result future
 * @param <B> type of the batch object
 * @param <R> type of the record
 */
public abstract class Reader<F extends CompletableFuture, B, R> {

    protected final String objectName;

    private final Function<B, Integer> toNextIndexFn;
    private Function<B, List<R>> toRecordSetFn;

    /**
     * Creates a reader for a map/cache
     */
    public Reader(@Nonnull String objectName,
           @Nonnull Function<B, Integer> toNextIndexFn,
           @Nonnull Function<B, List<R>> toRecordSetFn) {
        this.objectName = objectName;
        this.toNextIndexFn = toNextIndexFn;
        this.toRecordSetFn = toRecordSetFn;
    }

    /**
     * Read a batch from a partition with given offset
     *
     * @param partitionId id of the partition
     * @param offset offset to read
     *
     * @return a future
     */
    @Nonnull
    public abstract F readBatch(int partitionId, int offset);

    /**
     * Transform the future returned from {@link #readBatch(int, int)} to a batch
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public B toBatchResult(@Nonnull F future) throws ExecutionException, InterruptedException {
        return (B) future.get();
    }

    /**
     * Return the index that follows current batch
     */
    public final int toNextIndex(@Nonnull B result) {
        return toNextIndexFn.apply(result);
    }

    /**
     * Transforms batch result to a list of records
     */
    @Nonnull
    public final List<R> toRecordSet(@Nonnull B result) {
        return toRecordSetFn.apply(result);
    }

    /**
     * Transform a record to an object
     */
    @Nullable
    public abstract Object toObject(@Nonnull R record);

}
