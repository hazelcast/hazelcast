package com.hazelcast.jet.impl.connector;


import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Stateless interface to read a map/cache.
 *
 * @param <F> type of the result future
 * @param <B> type of the batch object
 * @param <R> type of the record
 */
abstract class AbstractMapReader<F extends CompletableFuture, B, R> {

    protected final String objectName;
    protected InternalSerializationService serializationService;

    protected final FunctionEx<B, IterationPointer[]> toNextIterationPointerFn;
    protected FunctionEx<B, List<R>> toRecordSetFn;

    AbstractMapReader(@Nonnull String objectName,
                      @Nonnull FunctionEx<B, IterationPointer[]> toNextIterationPointerFn,
                      @Nonnull FunctionEx<B, List<R>> toRecordSetFn) {
        this.objectName = objectName;
        this.toNextIterationPointerFn = toNextIterationPointerFn;
        this.toRecordSetFn = toRecordSetFn;
    }

    @Nonnull
    abstract F readBatch(int partitionId, IterationPointer[] pointers);

    @Nonnull
    @SuppressWarnings("unchecked")
    B toBatchResult(@Nonnull F future) throws ExecutionException, InterruptedException {
        return (B) future.get();
    }

    final IterationPointer[] toNextPointer(@Nonnull B result) {
        return toNextIterationPointerFn.apply(result);
    }

    @Nonnull
    final List<R> toRecordSet(@Nonnull B result) {
        return toRecordSetFn.apply(result);
    }

    @Nullable
    abstract Object toObject(@Nonnull R record);
}