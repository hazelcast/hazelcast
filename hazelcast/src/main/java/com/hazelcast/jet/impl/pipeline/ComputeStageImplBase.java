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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalFlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MergeTransform;
import com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.SortTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.filterUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServiceAsyncBatchedPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServiceAsyncPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.mapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.partitionedCustomProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.customProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.filterUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceAsyncBatchedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceAsyncTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.mapUsingServiceTransform;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class ComputeStageImplBase<T> extends AbstractStage {

    public static final FunctionAdapter ADAPT_TO_JET_EVENT = new JetEventFunctionAdapter();
    public static final int MAX_CONCURRENT_ASYNC_BATCHES = 2;
    static final FunctionAdapter DO_NOT_ADAPT = new FunctionAdapter();

    @Nonnull
    public final FunctionAdapter fnAdapter;
    final boolean isRebalanceOutput;
    final FunctionEx<? super T, ?> rebalanceKeyFn;

    private ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull FunctionAdapter fnAdapter,
            @Nonnull PipelineImpl pipelineImpl,
            boolean rebalanceOutput,
            FunctionEx<? super T, ?> rebalanceKeyFn
    ) {
        super(transform, pipelineImpl);
        this.fnAdapter = fnAdapter;
        this.isRebalanceOutput = rebalanceOutput;
        this.rebalanceKeyFn = rebalanceKeyFn;
    }

    ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull FunctionAdapter fnAdapter,
            @Nonnull PipelineImpl pipelineImpl
    ) {
        this(transform, fnAdapter, pipelineImpl, false, null);
    }

    ComputeStageImplBase(ComputeStageImplBase<T> toCopy, boolean rebalanceOutput) {
        this(toCopy.transform, toCopy.fnAdapter, toCopy.pipelineImpl, rebalanceOutput, null);
    }

    ComputeStageImplBase(ComputeStageImplBase<T> toCopy, FunctionEx<? super T, ?> rebalanceKeyFn) {
        this(toCopy.transform, toCopy.fnAdapter, toCopy.pipelineImpl, true, rebalanceKeyFn);
    }

    @Nonnull
    public StreamStage<T> addTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag) {
        checkTrue(fnAdapter.equals(DO_NOT_ADAPT), "This stage already has timestamps assigned to it");
        checkSerializable(timestampFn, "timestampFn");
        TimestampTransform<T> tsTransform = new TimestampTransform<>(transform, eventTimePolicy(
                timestampFn,
                (item, ts) -> jetEvent(ts, item),
                limitingLag(allowedLag),
                0, 0, DEFAULT_IDLE_TIMEOUT
        ));
        pipelineImpl.connect(this, tsTransform);
        return new StreamStageImpl<>(tsTransform, ADAPT_TO_JET_EVENT, pipelineImpl);
    }

    @Nonnull
    <RET> RET attachSort(@Nullable ComparatorEx<? super T> comparator) {
        return attach(new SortTransform<>(this.transform, comparator), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <R, RET> RET attachMap(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        checkSerializable(mapFn, "mapFn");
        return attach(new MapTransform("map", this.transform, fnAdapter.adaptMapFn(mapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked"})
    <RET> RET attachFilter(@Nonnull PredicateEx<T> filterFn) {
        checkSerializable(filterFn, "filterFn");
        PredicateEx<T> adaptedFn = (PredicateEx<T>) fnAdapter.adaptFilterFn(filterFn);
        return attach(new MapTransform<T, T>("filter", transform, t -> adaptedFn.test(t) ? t : null), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <R, RET> RET attachFlatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        return attach(new FlatMapTransform("flat-map", transform, fnAdapter.adaptFlatMapFn(flatMapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachGlobalMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(mapFn, "mapFn");
        GlobalMapStatefulTransform<T, S, R> mapStatefulTransform = new GlobalMapStatefulTransform(
                transform,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulMapFn((s, k, t) -> mapFn.apply(s, t))
        );
        return attach(mapStatefulTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachGlobalFlatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(flatMapFn, "flatMapFn");
        GlobalFlatMapStatefulTransform<T, S, R> mapStatefulTransform = new GlobalFlatMapStatefulTransform(
                transform,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulFlatMapFn((s, k, t) -> flatMapFn.apply(s, t))
        );
        return attach(mapStatefulTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <K, S, R, RET> RET attachMapStateful(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createFn, "createFn");
        checkSerializable(mapFn, "mapFn");
        if (ttl > 0 && fnAdapter == DO_NOT_ADAPT) {
            throw new IllegalStateException("Cannot use time-to-live on a non-timestamped stream");
        }
        MapStatefulTransform<T, K, S, R> mapStatefulTransform = new MapStatefulTransform(
                transform,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulMapFn(mapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFn(onEvictFn) : null);
        return attach(mapStatefulTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <K, S, R, RET> RET attachFlatMapStateful(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createFn, "createFn");
        checkSerializable(flatMapFn, "mapFn");
        if (ttl > 0 && fnAdapter == DO_NOT_ADAPT) {
            throw new IllegalStateException("Cannot use time-to-live on a non-timestamped stream");
        }
        FlatMapStatefulTransform<T, K, S, R> flatMapStatefulTransform = new FlatMapStatefulTransform(
                transform,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulFlatMapFn(flatMapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFlatMapFn(onEvictFn) : null);
        return attach(flatMapStatefulTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingServiceFn(mapFn);
        return attach(
                mapUsingServiceTransform(transform, serviceFactory, adaptedMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, RET> RET attachFilterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingServiceFn(filterFn);
        return attach(
                filterUsingServiceTransform(transform, serviceFactory, adaptedFilterFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachFlatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceFn(flatMapFn);
        return attach(
                flatMapUsingServiceTransform(transform, serviceFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachMapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, "mapAsyncFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        ProcessorTransform processorTransform = flatMapUsingServiceAsyncTransform(
                transform, "map", serviceFactory, maxConcurrentOps, preserveOrder, adaptedFlatMapFn);
        return attach(processorTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, R, RET> RET attachMapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>,
                    ? extends CompletableFuture<List<Traverser<R>>>> flatMapAsyncBatchedFn
    ) {
        checkSerializable(flatMapAsyncBatchedFn, "mapAsyncBatchedFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedFn = fnAdapter.adaptFlatMapUsingServiceAsyncBatchedFn(flatMapAsyncBatchedFn);

        // Here we flatten the result from List<Traverser<R>> to Traverser<R>.
        // The former is used in pipeline API, the latter in core API.
        BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> flattenedFn =
                (svc, items) -> {
                    // R might actually be JetEvent<R> -- we can't represent this with static types
                    CompletableFuture<List<Traverser<R>>> f =
                            (CompletableFuture<List<Traverser<R>>>) adaptedFn.apply(svc, items);
                    return f.thenApply(res -> traverseIterable(res).flatMap(Function.identity()));
                };

        ProcessorTransform processorTransform = flatMapUsingServiceAsyncBatchedTransform(
                transform, "map", serviceFactory, MAX_CONCURRENT_ASYNC_BATCHES, maxBatchSize, flattenedFn);
        return attach(processorTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, K, R, RET> RET attachMapUsingPartitionedService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingServiceFn(mapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                mapUsingServicePartitionedTransform(transform, serviceFactory, adaptedMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, K, RET> RET attachFilterUsingPartitionedService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingServiceFn(filterFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                filterUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFilterFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, K, R, RET> RET attachFlatMapUsingPartitionedService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceFn(flatMapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, K, R, RET> RET attachMapUsingPartitionedServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        checkSerializable(mapAsyncFn, "mapAsyncFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn =
                (s, t) -> mapAsyncFn.apply(s, t).thenApply(Traversers::singleton);
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        PartitionedProcessorTransform processorTransform = flatMapUsingServiceAsyncPartitionedTransform(
                transform,
                "map",
                serviceFactory,
                maxConcurrentOps,
                preserveOrder,
                adaptedFlatMapFn,
                adaptedPartitionKeyFn
        );
        return attach(processorTransform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <S, K, R, RET> RET attachMapUsingPartitionedServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    ) {
        checkSerializable(mapAsyncFn, "mapAsyncFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        serviceFactory = moveAttachedFilesToPipeline(serviceFactory);
        BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<Traverser<R>>>> flatMapAsyncFn =
                (s, items) -> mapAsyncFn.apply(s, items).thenApply(list -> toList(list, Traversers::singleton));
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceAsyncBatchedFn(flatMapAsyncFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);

        // Here we flatten the result from List<Traverser<R>> to Traverser<R>.
        // The former is used in pipeline API, the latter in core API.
        BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> flattenedFn =
                (svc, items) -> {
                    // R might actually be JetEvent<R> -- we can't represent this with static types
                    CompletableFuture<List<Traverser<R>>> f =
                            (CompletableFuture<List<Traverser<R>>>) adaptedFlatMapFn.apply(svc, items);
                    return f.thenApply(res -> traverseIterable(res).flatMap(Function.identity()));
                };

        PartitionedProcessorTransform processorTransform = flatMapUsingServiceAsyncBatchedPartitionedTransform(
                transform,
                "map",
                serviceFactory,
                MAX_CONCURRENT_ASYNC_BATCHES,
                maxBatchSize,
                flattenedFn,
                adaptedPartitionKeyFn
        );
        return attach(processorTransform, fnAdapter);
    }

    @Nonnull
    private <S> ServiceFactory<?, S> moveAttachedFilesToPipeline(@Nonnull ServiceFactory<?, S> serviceFactory) {
        pipelineImpl.attachFiles(serviceFactory.attachedFiles());
        return serviceFactory.withoutAttachedFiles();
    }

    @Nonnull
    @SuppressWarnings("rawtypes")
    <RET> RET attachMerge(@Nonnull GeneralStage<? extends T> other) {
        ComputeStageImplBase castOther = (ComputeStageImplBase) other;
        if (fnAdapter != castOther.fnAdapter) {
            throw new IllegalArgumentException(
                    "The merged stages must either both have or both not have timestamp definitions");
        }
        MergeTransform<Object> transform = new MergeTransform<>(this.transform, castOther.transform);
        return attach(transform, singletonList(other), fnAdapter);
    }

    @Nonnull
    <K1, T1_IN, T1, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attach(new HashJoinTransform<>(
                        asList(transform, transformOf(stage1)),
                        singletonList(fnAdapter.adaptJoinClause(joinClause)),
                        emptyList(),
                        fnAdapter.adaptHashJoinOutputFn(mapToOutputFn)
                ),
                singletonList(stage1),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <K1, T1_IN, T1, K2, T2_IN, T2, R, RET> RET attachHashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return attach(new HashJoinTransform(
                        asList(transform, transformOf(stage1), transformOf(stage2)),
                        asList(fnAdapter.adaptJoinClause(joinClause1), fnAdapter.adaptJoinClause(joinClause2)),
                        emptyList(),
                        fnAdapter.adaptHashJoinOutputFn(mapToOutputFn)
                ),
                asList(stage1, stage2),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <RET> RET attachPeek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        checkSerializable(shouldLogFn, "shouldLogFn");
        checkSerializable(toStringFn, "toStringFn");
        if (isRebalanceOutput) {
            throw new JetException("peek() not supported after rebalance()");
        }
        return attach(new PeekTransform(transform, fnAdapter.adaptFilterFn(shouldLogFn),
                fnAdapter.adaptToStringFn(toStringFn)
        ), fnAdapter);
    }

    @Nonnull
    <RET> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier
    ) {
        return attach(customProcessorTransform(stageName, transform, procSupplier), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    <K, RET> RET attachPartitionedCustomTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        FunctionEx adaptedKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                partitionedCustomProcessorTransform(stageName, transform, procSupplier, adaptedKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SinkStage writeTo(@Nonnull Sink<? super T> sink) {
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform<T> sinkTransform = new SinkTransform(sinkImpl, transform, fnAdapter == ADAPT_TO_JET_EVENT);
        SinkStageImpl output = new SinkStageImpl(sinkTransform, pipelineImpl);
        sinkImpl.onAssignToStage();
        pipelineImpl.connect(this, sinkTransform);
        return output;
    }

    @Nonnull
    final <RET> RET attach(
            @Nonnull AbstractTransform transform,
            @Nonnull List<? extends GeneralStage<?>> moreInputStages,
            @Nonnull FunctionAdapter fnAdapter
    ) {
        pipelineImpl.connect(this, moreInputStages, transform);
        return newStage(transform, fnAdapter);
    }

    @Nonnull
    final <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter) {
        return attach(transform, emptyList(), fnAdapter);
    }

    abstract <RET> RET newStage(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter);

    @SuppressWarnings("rawtypes")
    static void ensureJetEvents(@Nonnull ComputeStageImplBase stage, @Nonnull String name) {
        if (stage.fnAdapter != ADAPT_TO_JET_EVENT) {
            throw new IllegalStateException(
                    name + " is missing a timestamp definition. Call" +
                            " one of the .addTimestamps() methods on it before performing" +
                            " the aggregation."
            );
        }
    }
}
