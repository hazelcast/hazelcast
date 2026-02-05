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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A stage in a distributed computation {@link Pipeline pipeline} that will
 * observe an unbounded amount of data (i.e., an event stream). It accepts
 * input from its upstream stages (if any) and passes its output to its
 * downstream stages.
 *
 * @param <T> the type of items coming out of this stage
 *
 * @since Jet 3.0
 */
public interface StreamStage<T> extends GeneralStage<T> {

    /**
     * Adds the given window definition to this stage, as the first step in the
     * construction of a pipeline stage that performs windowed aggregation.
     *
     * @see WindowDefinition factory methods in WindowDefiniton
     */
    @Nonnull
    StageWithWindow<T> window(WindowDefinition wDef);

    /**
     * Attaches a stage that emits all the items from this stage as well as all
     * the items from the supplied stage. The other stage's type parameter must
     * be assignment-compatible with this stage's type parameter.
     *
     * @param other the other stage whose data to merge into this one
     * @return the newly attached stage
     */
    @Nonnull
    StreamStage<T> merge(@Nonnull StreamStage<? extends T> other);

    @Nonnull @Override
    <K> StreamStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <K> StreamStage<T> rebalance(@Nonnull FunctionEx<? super T, ? extends K> keyFn);

    @Nonnull @Override
    StreamStage<T> rebalance();

    @Nonnull @Override
    <R> StreamStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn);

    @Nonnull @Override
    StreamStage<T> filter(@Nonnull PredicateEx<T> filterFn);

    @Nonnull @Override
    <R> StreamStage<R> flatMap(@Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn);

    @Nonnull @Override
    <S, R> StreamStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <S> StreamStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn);

    @Nonnull @Override
    <S, R> StreamStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn);

    @Nonnull @Override
    default <A, R> StreamStage<R> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return (StreamStage<R>) GeneralStage.super.<A, R>rollingAggregate(aggrOp);
    }

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    default <S, R> StreamStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        return (StreamStage<R>) GeneralStage.super.mapUsingServiceAsync(serviceFactory, mapAsyncFn);
    }

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    <S> StreamStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    default <K, V, R> StreamStage<R> mapUsingReplicatedMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(mapName, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> StreamStage<R> mapUsingReplicatedMap(
            @Nonnull ReplicatedMap<K, V> replicatedMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(replicatedMap, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> StreamStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingIMap(mapName, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> StreamStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingIMap(iMap, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    <K, T1_IN, T1, R> StreamStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K, T1_IN, T1, R> StreamStage<R> innerHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> StreamStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> StreamStage<R> innerHashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    default StreamHashJoinBuilder<T> hashJoinBuilder() {
        return new StreamHashJoinBuilder<>(this);
    }

    @Nonnull @Override
    default StreamStage<T> peek() {
        return (StreamStage<T>) GeneralStage.super.peek();
    }

    @Nonnull @Override
    StreamStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn);

    @Nonnull @Override
    default StreamStage<T> peek(@Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn) {
        return (StreamStage<T>) GeneralStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName,
                                               @Nonnull SupplierEx<Processor> procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);

    /**
     * Transforms {@code this} stage using the provided {@code transformFn} and
     * returns the transformed stage. It allows you to extract common pipeline
     * transformations into a method and then call that method without
     * interrupting the chained pipeline expression.
     * <p>
     * For example, say you have this code:
     *
     * <pre>{@code
     * StreamStage<String> input = pipeline.readFrom(textSource);
     * StreamStage<String> cleanedUp = input
     *         .map(String::toLowerCase)
     *         .filter(s -> s.startsWith("success"));
     * }</pre>
     *
     * You can capture the {@code map} and {@code filter} steps into a common
     * "cleanup" transformation:
     *
     * <pre>{@code
     * StreamStage<String> cleanUp(StreamStage<String> input) {
     *      return input.map(String::toLowerCase)
     *                  .filter(s -> s.startsWith("success"));
     * }
     * }</pre>
     *
     * Now you can insert this transformation as just another step in your
     * pipeline:
     *
     * <pre>{@code
     * StreamStage<String> tokens = pipeline
     *     .readFrom(textSource)
     *     .apply(this::cleanUp)
     *     .flatMap(line -> traverseArray(line.split("\\W+")));
     * }</pre>
     *
     * @param transformFn function to transform this stage into another stage
     * @param <R> type of the returned stage
     *
     * @since Jet 3.1
     */
    @Nonnull
    default <R> StreamStage<R> apply(
            @Nonnull FunctionEx<? super StreamStage<T>, ? extends StreamStage<R>> transformFn
    ) {
        return transformFn.apply(this);
    }

    @Nonnull @Override
    StreamStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    StreamStage<T> setName(@Nonnull String name);

    /**
     * Extension of {@link StreamStage} providing additional capabilities
     * or convenience methods on top of the default stage API using fluent
     * interface.
     *
     * @param <T> the type of items produced by stage to be extended
     * @param <S> the type implementing additional methods for {@link StreamStage}
     *
     * @apiNote This interface in essence is a visitor pattern for different stage types.
     * @implNote Extension API type {@code S} should be an interface. It is not mandatory for
     * {@code S} to extend {@link StreamStage}. Single extension class can be
     * applicable to multiple stage types by implementing multiple interfaces.
     *
     * @since 5.7
     */
    interface StageExtension<T, S> {
        /**
         * Provides additional API to given stage in a fluent way.
         *
         * @param streamStage stage to be extended
         * @return extended stage
         *
         * @implNote The simplest implementation is just to wrap given stage
         * and use its methods to implement extension's methods.
         */
        @Nonnull
        S extend(@Nonnull StreamStage<T> streamStage);
    }

    /**
     * Applies extension to this stage in a fluent way.
     *
     * <pre>{@code
     * srcStage.using(IMapExtension.iMapExtension())
     *         // additional methods available here
     *         .mapUsingPutIfAbsent("my-map", e -> e % 10, e -> e + 5, Tuple3::tuple3)
     *         // Depending on the implementation we can be back to regular stage API
     *         // and need to invoke `using` again to get extra methods.
     *         // Alternatively extension may continue its scope until explicitly exited.
     *         .using(IMapExtension.iMapExtension())
     *         // additional methods available here
     *         .mapUsingPutIfAbsent("my-map2", e -> e.f0() % 10, e -> e.f0() + 5, Tuple3::tuple3)
     *         .writeTo(Sinks.logger());
     * }</pre>
     * <p>
     * Usually if the extension creates a pipeline stage, the new stage should be returned
     * at the end so the pipeline can be continued. {@link #setName(String)} and
     * {@link #setLocalParallelism(int)} can be used to adjust such stage
     * in the usual way, for example:
     * <pre>{@code
     * srcStage.using(IMapExtension.iMapExtension())
     *         .mapUsingPutIfAbsent("my-map", e -> e % 10, e -> e + 5, Tuple3::tuple3)
     *         .setName("check-if-known").setLocalParallelism(2);
     * }</pre>
     *
     * @param extension extension to be applied to this stage
     * @param <S> the type implementing additional methods for {@link StreamStage}
     * @return extended stage
     *
     * @since 5.7
     */
    @Nonnull
    default <S> S using(@Nonnull StageExtension<T, S> extension) {
        Objects.requireNonNull(extension, "extension cannot be null");
        return extension.extend(this);
    }
}
