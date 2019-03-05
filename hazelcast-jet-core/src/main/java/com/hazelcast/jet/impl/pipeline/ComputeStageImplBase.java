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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.FilterTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalRollingAggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MergeTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.RollingAggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.filterUsingPartitionedContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingPartitionedContextAsyncTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingPartitionedContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.mapUsingContextPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.partitionedCustomProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.customProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.filterUsingContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingContextAsyncTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.mapUsingContextTransform;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class ComputeStageImplBase<T> extends AbstractStage {

    static final FunctionAdapter DO_NOT_ADAPT = new FunctionAdapter();
    static final JetEventFunctionAdapter ADAPT_TO_JET_EVENT = new JetEventFunctionAdapter();

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
        return (RET) attach(new MapTransform(this.transform, fnAdapter.adaptMapFn(mapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachFilter(@Nonnull PredicateEx<T> filterFn) {
        checkSerializable(filterFn, "filterFn");
        return (RET) attach(new FilterTransform(transform, fnAdapter.adaptFilterFn(filterFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachFlatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        return (RET) attach(new FlatMapTransform(transform, fnAdapter.adaptFlatMapFn(flatMapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, RET> RET attachMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingContextFn(mapFn);
        return (RET) attach(
                mapUsingContextTransform(transform, contextFactory, adaptedMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, RET> RET attachFilterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingContextFn(filterFn);
        return (RET) attach(
                filterUsingContextTransform(transform, contextFactory, adaptedFilterFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, RET> RET attachFlatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextFn(flatMapFn);
        return (RET) attach(
                flatMapUsingContextTransform(transform, contextFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, RET> RET attachFlatMapUsingContextAsync(
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextAsyncFn(flatMapAsyncFn);
        return (RET) attach(
                flatMapUsingContextAsyncTransform(transform, operationName, contextFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, R, RET> RET attachMapUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedMapFn = fnAdapter.adaptMapUsingContextFn(mapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                mapUsingContextPartitionedTransform(transform, contextFactory, adaptedMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, RET> RET attachFilterUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiPredicateEx<? super C, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiPredicateEx adaptedFilterFn = fnAdapter.adaptFilterUsingContextFn(filterFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                filterUsingPartitionedContextTransform(
                        transform, contextFactory, adaptedFilterFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, R, RET> RET attachFlatMapUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextFn(flatMapFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingPartitionedContextTransform(
                        transform, contextFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, R, RET> RET attachTransformUsingPartitionedContextAsync(
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn,
            @Nonnull BiFunctionEx<? super C, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        checkSerializable(flatMapAsyncFn, operationName + "AsyncFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        BiFunctionEx adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextAsyncFn(flatMapAsyncFn);
        FunctionEx adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingPartitionedContextAsyncTransform(
                        transform, operationName, contextFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, R, OUT, RET> RET attachRollingAggregate(
            FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, ? extends OUT> mapToOutputFn

    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return (RET) attach(new RollingAggregateTransform(
                        transform,
                        fnAdapter.adaptKeyFn(keyFn),
                        fnAdapter.adaptAggregateOperation1(aggrOp),
                        fnAdapter.adaptRollingAggregateOutputFn(mapToOutputFn)
        ), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachGlobalRollingAggregate(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp) {
        GlobalRollingAggregateTransform transform = new GlobalRollingAggregateTransform(
                this.transform,
                fnAdapter.adaptAggregateOperation1(aggrOp),
                fnAdapter.adaptRollingAggregateOutputFn((key, result) -> result));
        return (RET) attach(transform, fnAdapter);
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
