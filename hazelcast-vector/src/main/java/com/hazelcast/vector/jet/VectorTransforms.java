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

package com.hazelcast.vector.jet;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.security.permission.VectorCollectionPermission;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.security.Permission;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;

/**
 * Transforms allowing to execute similarity search in Jet pipeline to enrich the stream.
 *
 * <p>
 * Example usage:
 *
 * <pre>{@code
 * StreamStage<String> inputStage = createInputStage();
 * StreamStage<String> outputStage = inputStage.apply(
 *         mapUsingVectorSearch(collectionName,
 *                         // search options template
 *                         SearchOptions.of(new float[]{0}, 1, true, false),
 *                         // extract vector from the item
 *                         item -> item.getVector(),
 *                         // use results to enrich the item or replace it
 *                         (item, results) -> Map.entry(item, results)
 *                 ));
 * }</pre>
 *
 * With batch stages, {@link #mapUsingVectorSearchBatch} has to be used instead {@link #mapUsingVectorSearch}.
 *
 * @since 5.5
 */
@Beta
public final class VectorTransforms {
    private VectorTransforms() {
    }

    /**
     * A stage-transforming method that adds similarity search pipeline stage for streaming pipelines.
     *
     * @param collectionName vector collection name
     * @param options search options prototype (vectors will be supplied for each item)
     * @param toVectorFn function that extracts the vectors from the stream item. It must be stateless and
     *                   {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param resultFn function that aggregates original item and search results. Search results are never null
     *                 but may be empty. It must be stateless and
     *                 {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param <T> type of incoming stream item
     * @param <K> type of vector collection key
     * @param <V> type of vector collection value
     * @param <R> type of produced stream item
     * @return the newly attached stage
     */
    @Nonnull
    public static <T, K, V, R> FunctionEx<StreamStage<T>, StreamStage<R>> mapUsingVectorSearch(
            @Nonnull String collectionName,
            @Nonnull SearchOptions options,
            @Nonnull FunctionEx<T, VectorValues> toVectorFn,
            @Nonnull BiFunctionEx<T, SearchResults<K, V>, R> resultFn
    ) {
        return s -> s.mapUsingServiceAsync(
                factory(collectionName),
                (collection, item) -> search((VectorCollection<K, V>) collection, options, toVectorFn, resultFn, item)
        ).setName("mapUsingVectorCollectionSearch");
    }

    /**
     * A stage-transforming method that adds similarity search pipeline stage for streaming pipelines.
     *
     * @param collection vector collection
     * @param options search options prototype (vectors will be supplied for each item)
     * @param toVectorFn function that extracts the vectors from the stream item. It must be stateless and
     *                   {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param resultFn function that aggregates original item and search results. Search results are never null
     *                 but may be empty. It must be stateless and
     *                 {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param <T> type of incoming stream item
     * @param <K> type of vector collection key
     * @param <V> type of vector collection value
     * @param <R> type of produced stream item
     * @return the newly attached stage
     */
    @Nonnull
    public static <T, K, V, R> FunctionEx<StreamStage<T>, StreamStage<R>> mapUsingVectorSearch(
            @Nonnull VectorCollection<K, V> collection,
            @Nonnull SearchOptions options,
            @Nonnull FunctionEx<T, VectorValues> toVectorFn,
            @Nonnull BiFunctionEx<T, SearchResults<K, V>, R> resultFn
    ) {
        return mapUsingVectorSearch(collection.getName(), options, toVectorFn, resultFn);
    }

    /**
     * A stage-transforming method that adds similarity search pipeline stage for batch pipelines.
     *
     * @param collectionName vector collection name
     * @param options search options prototype (vectors will be supplied for each item)
     * @param toVectorFn function that extracts the vectors from the stream item. It must be stateless and
     *                   {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param resultFn function that aggregates original item and search results. Search results are never null
     *                 but may be empty. It must be stateless and
     *                 {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param <T> type of incoming stream item
     * @param <K> type of vector collection key
     * @param <V> type of vector collection value
     * @param <R> type of produced stream item
     * @return the newly attached stage
     */
    @Nonnull
    public static <T, K, V, R> FunctionEx<BatchStage<T>, BatchStage<R>> mapUsingVectorSearchBatch(
            @Nonnull String collectionName,
            @Nonnull SearchOptions options,
            @Nonnull FunctionEx<T, VectorValues> toVectorFn,
            @Nonnull BiFunctionEx<T, SearchResults<K, V>, R> resultFn
            ) {
        return s -> s.mapUsingServiceAsync(
                factory(collectionName),
                (collection, item) -> search((VectorCollection<K, V>) collection, options, toVectorFn, resultFn, item)
        ).setName("mapUsingVectorCollectionSearch");
    }

    /**
     * A stage-transforming method that adds similarity search pipeline stage for batch pipelines.
     *
     * @param collection vector collection
     * @param options search options prototype (vectors will be supplied for each item)
     * @param toVectorFn function that extracts the vectors from the stream item. It must be stateless and
     *                   {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param resultFn function that aggregates original item and search results. Search results are never null
     *                 but may be empty. It must be stateless and
     *                 {@linkplain com.hazelcast.jet.core.Processor#isCooperative() cooperative}
     * @param <T> type of incoming stream item
     * @param <K> type of vector collection key
     * @param <V> type of vector collection value
     * @param <R> type of produced stream item
     * @return the newly attached stage
     */
    @Nonnull
    public static <T, K, V, R> FunctionEx<BatchStage<T>, BatchStage<R>> mapUsingVectorSearchBatch(
            @Nonnull VectorCollection<K, V> collection,
            @Nonnull SearchOptions options,
            @Nonnull FunctionEx<T, VectorValues> toVectorFn,
            @Nonnull BiFunctionEx<T, SearchResults<K, V>, R> resultFn
    ) {
        return mapUsingVectorSearchBatch(collection.getName(), options, toVectorFn, resultFn);
    }

    private static <T, K, V, R> CompletableFuture<R> search(@Nonnull VectorCollection<K, V> collection,
                                                            @Nonnull SearchOptions options,
                                                            @Nonnull FunctionEx<T, VectorValues> toVectorFn,
                                                            @Nonnull BiFunctionEx<T, SearchResults<K, V>, R> resultFn,
                                                            T item) {
        return collection.searchAsync(toVectorFn.apply(item), options)
                .toCompletableFuture()
                .thenApplyAsync(sr -> resultFn.apply(item, sr), CALLER_RUNS);
    }

    private static <K, V> ServiceFactory<?, VectorCollection<K, V>> factory(@Nonnull String collectionName) {
        return ServiceFactories.sharedService(VectorTransforms.<K, V>vectorCollectionFn(collectionName))
                .withPermission(vectorCollectionSearchPermission(collectionName));
    }

    private static <K, V> FunctionEx<? super ProcessorSupplier.Context, VectorCollection<K, V>> vectorCollectionFn(String name) {
        return new FunctionEx<>() {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            public VectorCollection<K, V> applyEx(ProcessorSupplier.Context context) {
                return context.hazelcastInstance().getVectorCollection(name);
            }

            @Override
            public List<Permission> permissions() {
                return singletonList(vectorCollectionSearchPermission(name));
            }
        };
    }

    private static VectorCollectionPermission vectorCollectionSearchPermission(String collectionName) {
        return new VectorCollectionPermission(collectionName, ACTION_CREATE, ACTION_READ);
    }
}
