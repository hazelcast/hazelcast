/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.FilterTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.MergeTransform;
import com.hazelcast.jet.impl.pipeline.transform.PeekTransform;
import com.hazelcast.jet.impl.pipeline.transform.RollingAggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.SinkTransform;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
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

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.NULL_EMIT_POLICY;
import static com.hazelcast.jet.core.WatermarkGenerationParams.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.filterUsingPartitionedContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.flatMapUsingPartitionedContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.mapUsingContextPartitionedTransform;
import static com.hazelcast.jet.impl.pipeline.transform.PartitionedProcessorTransform.partitionedCustomProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.customProcessorTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.filterUsingContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.flatMapUsingContextTransform;
import static com.hazelcast.jet.impl.pipeline.transform.ProcessorTransform.mapUsingContextTransform;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkFalse;
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
    public StreamStage<T> addTimestamps() {
        return addTimestamps(o -> System.currentTimeMillis(), 0);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public StreamStage<T> addTimestamps(
            @Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLateness
    ) {
        checkSerializable(timestampFn, "timestampFn");
        checkFalse(hasJetEvents(), "This stage already has timestamps assigned to it.");

        DistributedSupplier<WatermarkPolicy> wmPolicy = limitingLag(allowedLateness);
        WatermarkGenerationParams<T> wmParams = wmGenParams(
                timestampFn, JetEvent::jetEvent, wmPolicy, NULL_EMIT_POLICY, DEFAULT_IDLE_TIMEOUT
        );

        if (transform instanceof StreamSourceTransform) {
            ((StreamSourceTransform<T>) transform).setWmGenerationParams(wmParams);
            this.fnAdapter = ADAPT_TO_JET_EVENT;
            return (StreamStage<T>) this;
        }
        TimestampTransform<T> tsTransform = new TimestampTransform<>(transform, wmParams);
        pipelineImpl.connect(transform, tsTransform);
        return new StreamStageImpl<>(tsTransform, ADAPT_TO_JET_EVENT, pipelineImpl);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachMap(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        checkSerializable(mapFn, "mapFn");
        return (RET) attach(new MapTransform(this.transform, fnAdapter.adaptMapFn(mapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <RET> RET attachFilter(@Nonnull DistributedPredicate<T> filterFn) {
        checkSerializable(filterFn, "filterFn");
        return (RET) attach(new FilterTransform(transform, fnAdapter.adaptFilterFn(filterFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, RET> RET attachFlatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        return (RET) attach(new FlatMapTransform(transform, fnAdapter.adaptFlatMapFn(flatMapFn)), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, RET> RET attachMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        DistributedBiFunction adaptedMapFn = fnAdapter.adaptMapUsingContextFn(mapFn);
        return (RET) attach(
                mapUsingContextTransform(transform, contextFactory, adaptedMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, RET> RET attachFilterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        DistributedBiPredicate adaptedFilterFn = fnAdapter.adaptFilterUsingContextFn(filterFn);
        return (RET) attach(
                filterUsingContextTransform(transform, contextFactory, adaptedFilterFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, RET> RET attachFlatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        DistributedBiFunction adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextFn(flatMapFn);
        return (RET) attach(
                flatMapUsingContextTransform(transform, contextFactory, adaptedFlatMapFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, R, RET> RET attachMapUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        checkSerializable(mapFn, "mapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        DistributedBiFunction adaptedMapFn = fnAdapter.adaptMapUsingContextFn(mapFn);
        DistributedFunction adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                mapUsingContextPartitionedTransform(transform, contextFactory, adaptedMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, RET> RET attachFilterUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        checkSerializable(filterFn, "filterFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        DistributedBiPredicate adaptedFilterFn = fnAdapter.adaptFilterUsingContextFn(filterFn);
        DistributedFunction adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                filterUsingPartitionedContextTransform(
                        transform, contextFactory, adaptedFilterFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, K, R, RET> RET attachFlatMapUsingPartitionedContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        checkSerializable(partitionKeyFn, "partitionKeyFn");
        DistributedBiFunction adaptedFlatMapFn = fnAdapter.adaptFlatMapUsingContextFn(flatMapFn);
        DistributedFunction adaptedPartitionKeyFn = fnAdapter.adaptKeyFn(partitionKeyFn);
        return (RET) attach(
                flatMapUsingPartitionedContextTransform(
                        transform, contextFactory, adaptedFlatMapFn, adaptedPartitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K, R, OUT, RET> RET attachRollingAggregate(
            DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn

    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        return (RET) attach(new RollingAggregateTransform(
                        transform,
                        fnAdapter.adaptKeyFn(keyFn),
                        fnAdapter.adaptAggregateOperation1(aggrOp),
                        fnAdapter.adaptRollingAggregateOutputFn(mapToOutputFn)),
                fnAdapter);
    }

    @Nonnull
    <RET> RET attachMerge(@Nonnull GeneralStage<? extends T> other) {
        return attach(new MergeTransform<>(transform, ((AbstractStage) other).transform), fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <K1, T1_IN, T1, R, RET> RET attachHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
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
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
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
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        checkSerializable(shouldLogFn, "shouldLogFn");
        checkSerializable(toStringFn, "toStringFn");
        return attach(new PeekTransform(transform, fnAdapter.adaptFilterFn(shouldLogFn),
                fnAdapter.adaptToStringFn(toStringFn)), fnAdapter);
    }

    @Nonnull
    <RET> RET attachCustomTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attach(customProcessorTransform(stageName, transform, procSupplier), fnAdapter);
    }

    @Nonnull
    <K, RET> RET attachPartitionedCustomTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorSupplier procSupplier,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn

    ) {
        return attach(partitionedCustomProcessorTransform(stageName, transform, procSupplier, partitionKeyFn),
                fnAdapter);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public SinkStage drainTo(@Nonnull Sink<? super T> sink) {
        SinkImpl sinkImpl = (SinkImpl) sink;
        SinkTransform<T> sinkTransform = new SinkTransform(
                sinkImpl, transform, fnAdapter == ADAPT_TO_JET_EVENT);
        SinkStageImpl output = new SinkStageImpl(sinkTransform, pipelineImpl);
        sinkImpl.onAssignToStage();
        pipelineImpl.connect(transform, sinkTransform);
        return output;
    }

    @Nonnull
    abstract <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter);

    private boolean hasJetEvents() {
        return fnAdapter.equals(ADAPT_TO_JET_EVENT);
    }

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
