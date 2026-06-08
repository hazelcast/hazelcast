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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.jet.impl.WriteVectorCollectionP;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

/**
 * Factory methods for Vector collection sinks
 *
 * @since 5.5
 */
@Beta
public final class VectorSinks {
    private VectorSinks() {
    }

    /**
     * Returns a sink that writes vector documents with metadata and vector values
     * to a vector collection.
     * <p>
     * This sink internally limits number of parallel requests to not overload the
     * members.
     *
     * @param collection     vector collection
     * @param toKeyFn        function that extracts the key from the stream item
     * @param toValueFn      function that extracts the value from the stream item
     * @param toVectorsFn    function that extracts the vectors from the stream item
     * @param <T>            type of stream item
     * @param <K>            type of vector collection key
     * @param <V>            type of vector collection value
     */
    @Nonnull
    public static <T, K, V> Sink<T> vectorCollection(@Nonnull VectorCollection<K, V> collection,
                                                     @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                                                     @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
                                                     @Nonnull FunctionEx<? super T, VectorValues> toVectorsFn) {
        return vectorCollection(collection.getName(), toKeyFn, toValueFn, toVectorsFn);
    }

    /**
     * Returns a sink that writes vector documents with metadata and vector values
     * to a vector collection.
     * <p>
     * This sink internally limits number of parallel requests to not overload the
     * members.
     *
     * @param collection     vector collection
     * @param toKeyFn        function that extracts the key from the stream item
     * @param toDocumentFn   function that creates {@link VectorDocument} based on the stream item
     * @param <T>            type of stream item
     * @param <K>            type of vector collection key
     * @param <V>            type of vector collection value
     */
    @Nonnull
    public static <T, K, V> Sink<T> vectorCollection(@Nonnull VectorCollection<K, V> collection,
                                                     @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                                                     @Nonnull FunctionEx<? super T,
                                                             VectorDocument<? extends V>> toDocumentFn) {
        return vectorCollection(collection.getName(), toKeyFn, toDocumentFn);
    }

    /**
     * Returns a sink that writes vector documents with metadata and vector values
     * to a vector collection.
     * <p>
     * This sink internally limits number of parallel requests to not overload the
     * members.
     *
     * @param collectionName vector collection name
     * @param toKeyFn        function that extracts the key from the stream item
     * @param toValueFn      function that extracts the value from the stream item
     * @param toVectorsFn    function that extracts the vectors from the stream item
     * @param <T>            type of stream item
     * @param <K>            type of vector collection key
     * @param <V>            type of vector collection value
     */
    @Nonnull
    public static <T, K, V> Sink<T> vectorCollection(@Nonnull String collectionName,
                                                     @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                                                     @Nonnull FunctionEx<? super T, ? extends V> toValueFn,
                                                     @Nonnull FunctionEx<? super T, VectorValues> toVectorsFn) {
        return vectorCollection(collectionName, toKeyFn,
                item -> VectorDocument.of(toValueFn.apply(item), toVectorsFn.apply(item)));
    }

    /**
     * Returns a sink that writes vector documents with metadata and vector values
     * to a vector collection.
     * <p>
     * This sink internally limits number of parallel requests to not overload the
     * members.
     *
     * @param collectionName vector collection name
     * @param toKeyFn        function that extracts the key from the stream item
     * @param toDocumentFn   function that creates {@link VectorDocument} based on the stream item
     * @param <T>            type of stream item
     * @param <K>            type of vector collection key
     * @param <V>            type of vector collection value
     */
    @Nonnull
    public static <T, K, V> Sink<T> vectorCollection(@Nonnull String collectionName,
                                                     @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
                                                     @Nonnull FunctionEx<? super T,
                                                             VectorDocument<? extends V>> toDocumentFn) {
        return Sinks.fromProcessor("vectorCollectionSink(" + collectionName + ")",
                // internally uses async putAll requests and limits parallelism there
                preferLocalParallelismOne(WriteVectorCollectionP.vectorCollectionPutPermission(collectionName),
                        new WriteVectorCollectionP.Supplier<>(collectionName, toKeyFn, toDocumentFn)),
                toKeyFn);
    }
}
