/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
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
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
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
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.filterUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServiceAsyncPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.mapUsingServicePartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.partitionedCustomProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.customProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.filterUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceAsyncTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingServiceTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.mapUsingServiceTransform;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class ComputeStageImplBase<T> extends AbstractStage {

    public static final FunctionAdapter ADAPT_TO_JET_EVENT = new JetEventFunctionAdapter();
    static final FunctionAdapter DO_NOT_ADAPT = new FunctionAdapter();

    @Nonnull
    public FunctionAdapter fnAdapter;

    ComputeStageImplBase(
            @Nonnull Transform transform,
            @Nonnull FunctionAdapter fnAdapter,
            @Nonnull PipelineImpl pipelineImpl,
            boolean acceptsDownstream
    ) {
        super(transform, acceptsDownstream, pipelineImpl);
        this.fnAdapter = fnAdapter;
    }

    @Nonnull
    public StreamStage<T> addTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLateness) {
        checkTrue(fnAdapter.equals(DO_NOT_ADAPT), "This stage already has timestamps assigned to it");
        checkSerializable(timestampFn, "timestampFn");
        TimestampTransform<T> tsTransform = new TimestampTransform<>(transform, eventTimePolicy(
                timestampFn,
                (item, ts) -> jetEvent(ts, item),
                limitingLag(allowedLateness),
                0, 0, DEFAULT_IDLE_TIMEOUT
        ));
        pipelineImpl.connect(transform, tsTransform);
        return new StreamStageImpl<>(tsTransform, ADAPT_TO_JET_EVENT, pipelineImpl);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachMap(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        checkSerializable(mapFn, "mapFn");
        return (RET) attach(new MapTransform("map", this.transform, fnAdapter.adaptMapFn(mapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachFilter(@Nonnull PredicateEx<T> filterFn) {
        checkSerializable(filterFn, "filterFn");
        PredicateEx<T> adaptedFn = (PredicateEx<T>) fnAdapter.adaptFilterFn(filterFn);
        return (RET) attach(new MapTransform<T, T>("filter", transform, t -> adaptedFn.test(t) ? t : null), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachFlatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        return (RET) attach(new FlatMapTransform("flat-map", transform, fnAdapter.adaptFlatMapFn(flatMapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachGlobalMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(mapFn, "mapFn");
        GlobalMapStatefulTransform<T, S, R> transform = new GlobalMapStatefulTransform(
                this.transform,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulMapFn((s, k, t) -> mapFn.apply(s, t))
        );
        return (RET) attach(transform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachGlobalFlatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(flatMapFn, "flatMapFn");
        GlobalFlatMapStatefulTransform<T, S, R> transform = new GlobalFlatMapStatefulTransform(
                this.transform,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulFlatMapFn((s, k, t) -> flatMapFn.apply(s, t))
        );
        return (RET) attach(transform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
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
        MapStatefulTransform<T, K, S, R> transform = new MapStatefulTransform(
                this.transform,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulMapFn(mapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFn(onEvictFn) : null);
        return (RET) attach(transform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
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
        FlatMapStatefulTransform<T, K, S, R> transform = new FlatMapStatefulTransform(
                this.transform,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulFlatMapFn(flatMapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFlatMapFn(onEvictFn) : null);
        return (RET) attach(transform, fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachMapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingServiceFn(mapFn);
        return (RET) attach(
                mapUsingServiceTransform(transform, serviceFactory, adaptedMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, RET> RET attachFilterUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingServiceFn(filterFn);
        return (RET) attach(
                filterUsingServiceTransform(transform, serviceFactory, adaptedFilterFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachFlatMapUsingService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceFn(flatMapFn);
        return (RET) attach(
                flatMapUsingServiceTransform(transform, serviceFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, R, RET> RET attachFlatMapUsingServiceAsync(
            @Nonnull String operationName,
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        return (RET) attach(
                flatMapUsingServiceAsyncTransform(transform, operationName, serviceFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachMapUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingServiceFn(mapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                mapUsingServicePartitionedTransform(transform, serviceFactory, adaptedMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, RET> RET attachFilterUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingServiceFn(filterFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                filterUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFilterFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachFlatMapUsingPartitionedService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceFn(flatMapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingServicePartitionedTransform(
                        transform, serviceFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <S, K, R, RET> RET attachTransformUsingPartitionedServiceAsync(
            @Nonnull String operationName,
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingServiceAsyncFn(flatMapAsyncFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingServiceAsyncPartitionedTransform(
                        transform, operationName, serviceFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    <RET> RET attachMerge(@Nonnull GeneralStage<? extends T> other) {
        return attach(new MergeTransform<>(transform, ((AbstractStage) other).transform), fnAdapter);
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
        ), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, K2, T2_IN, T2, R, TA, RET> RET attachHashJoin2(
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
        ), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachPeek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        checkSerializable(shouldLogFn, "shouldLogFn");
        checkSerializable(toStringFn, "toStringFn");
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
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
    public SinkStage drainTo(@Nonnull Sink<? super T> sink) {
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform<T> sinkTransform = new SinkTransform(sinkImpl, transform, fnAdapter == ADAPT_TO_JET_EVENT);
        SinkStageImpl output = new SinkStageImpl(sinkTransform, pipelineImpl);
        sinkImpl.onAssignToStage();
        pipelineImpl.connect(transform, sinkTransform);
        return output;
    }

    @Nonnull
    abstract <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter);

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
