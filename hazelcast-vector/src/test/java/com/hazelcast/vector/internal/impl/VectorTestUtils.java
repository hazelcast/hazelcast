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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.storage.ArrayVectorProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static com.hazelcast.test.HazelcastTestSupport.generateKeyForPartition;

public class VectorTestUtils {
    public static <K, V, R> Map<K, R> toMap(SearchResults<K, V> results, Function<SearchResult<K, V>, R> valueMapper) {
        return StreamSupport.stream(Spliterators.spliterator(results.results(), results.size(), 0), false)
                        .collect(Collectors.toMap(sr -> (K) sr.getKey(), valueMapper,
                                (u, v) -> {
                                    throw new IllegalStateException("Duplicate key: " + u);
                                },
                                // preserve order or results
                                LinkedHashMap::new));
    }

    public static <K, V> Map<K, ? extends SearchResult<K, V>> toMap(SearchResults<K, V> results) {
        return toMap(results, Function.identity());
    }


    // fake generic to accommodate all places where it will be used
    public static <T extends SearchResult<String, ?>> T sr(String key, float score) {
        return (T) new SearchResultImpl<>(0, key, score);
    }

    public static SearchResult<Data, Data> srData(String key, float score) {
        return new SearchResultImpl<>(0, heapData(key), score);
    }

    public static Data heapData(String key) {
        // note: this is not a properly formatted heap data, has wrong hash and type but should be sufficient for the tests
        // Heap data contains 8-byte header
        return new HeapData(("01234567" + key).getBytes(StandardCharsets.UTF_8));
    }

    public static <K, T extends SearchResult> T sr(K key, float score, VectorValues vectorValues) {
        return (T) new SearchResultImpl<>(0, key, score).setVectors(vectorValues);
    }

    public static <K, T extends SearchResult<K, V>, V> T sr(K key, float score, V value, VectorValues vectorValues) {
        return (T) new SearchResultImpl<>(0, key, score).setValue(value).setVectors(vectorValues);
    }

    /**
     * @return single, unnamed vector with given coordinates
     */
    public static VectorValues vec(float... coordinates) {
        return VectorValues.of(coordinates);
    }

    /**
     * @return single, named vector with given coordinates
     */
    public static VectorValues vec(String name, float... coordinates) {
        return VectorValues.of(name, coordinates);
    }

    /**
     * @return vector of given dimension with random coordinates
     */
    public static VectorValues randomVec(int dimension) {
        return VectorValues.of(randomFloatArray(dimension));
    }

    /**
     * @return search results consisting of given individual results
     */
    @SafeVarargs
    public static <K, V> SearchResults<K, V> srs(SearchResult<K, V>... srs) {
        return new SearchResultsImpl<>(List.of(srs));
    }

    /**
     * Shortcut for defining recursive comparison that:
     * <ol>
     *     <li>compares top-level object field-by-field</li>
     *     <li>uses {@link Object#equals} where overridden on fields, otherwise compares fields recursively</li>
     *     <li>ignores some fields</li>
     * </ol>
     * The main usage for this method is when top-level object implements {@link Object#equals}
     * that does not meet requirements of the test, but fields have meaningful, appropriate
     * {@link Object#equals} implementation preferred over field-by-field comparison.
     */
    public static RecursiveComparisonConfiguration usingOverriddenEqualsIgnoringFields(String... fieldsToIgnore) {
        return RecursiveComparisonConfiguration.builder()
                .withIgnoreAllOverriddenEquals(false)
                .withIgnoredFields(fieldsToIgnore)
                .build();
    }

    /**
     * @return vector of given dimension with random coordinates
     */
    public static float[] randomFloatArray(int dimension) {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = ThreadLocalRandom.current().nextFloat();
            if (ThreadLocalRandom.current().nextBoolean()) {
                vector[i] = -vector[i];
            }
        }
        return vector;
    }

    /**
     * @return vector of given dimension with random coordinates
     */
    public static VectorFloat<?> randomVectorFloat(int dimension) {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = ThreadLocalRandom.current().nextFloat();
            if (ThreadLocalRandom.current().nextBoolean()) {
                vector[i] = -vector[i];
            }
        }
        return toVectorFloat(vector);
    }

    public static VectorFloat<?> ensureNonZero(VectorFloat<?> vector) {
        // easy way to make sure that the vector is not 0 length which could cause division errors
        // especially with small dimensions
        if (vector.get(0) == 0) {
            vector.set(0, 0.1f);
        }
        return vector;
    }

    /**
     * Insert one key per partition, ensuring vector collection storage exists for all partitions.
     */
    public static void warmupOneIndexCollection(
            HazelcastInstance member,
            VectorCollection<String, String> collection
    ) {
        var dimension = member.getConfig().getVectorCollectionConfigOrNull(collection.getName()).getVectorIndexConfigs().get(0).getDimension();
        var partitionCount = member.getPartitionService().getPartitions().size();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            collection.putAsync(
                    generateKeyForPartition(member, partitionId), VectorDocument.of("value", randomVec(dimension))
            ).toCompletableFuture().join();
        }
    }

    /**
     * Create VectorCollection and insert one key per partition, ensuring vector collection storage exists for all partitions.
     */
    public static VectorCollection<String, String> warmupCollection(HazelcastInstance member,
                                                                    String collectionName, int dimension, Metric metric) {
        var config = buildVectorCollectionConfig(collectionName, "default", dimension, metric);
        member.getConfig().addVectorCollectionConfig(config);
        var collection = member.<String, String>getVectorCollection(config.getName());
        warmupOneIndexCollection(member, collection);
        return collection;
    }

    /**
     * Insert one key per partition for each index, ensuring vector collection storage exists for all partitions.
     *
     * @return the map of added entries
     */
    public static Map<String, VectorDocument<String>> warmupTwoIndexCollection(
            HazelcastInstance member,
            VectorCollection<String, String> collection
    ) {
        var index1 = member.getConfig().getVectorCollectionConfigOrNull(collection.getName()).getVectorIndexConfigs().get(0);
        var index2 = member.getConfig().getVectorCollectionConfigOrNull(collection.getName()).getVectorIndexConfigs().get(1);
        var partitionCount = member.getPartitionService().getPartitions().size();
        Map<String, VectorDocument<String>> entries = new HashMap<>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            var key = generateKeyForPartition(member, partitionId);
            var document = VectorDocument.of(
                    "value",
                    VectorValues.of(index1.getName(), randomFloatArray(index1.getDimension()),
                            index2.getName(), randomFloatArray(index2.getDimension()))
            );
            collection.putAsync(key, document).toCompletableFuture().join();
            entries.put(key, document);
        }
        return entries;
    }

    public static Data randomData(int bytesLength) {
        byte[] bytes = new byte[bytesLength];
        ThreadLocalRandom.current().nextBytes(bytes);
        return new HeapData(bytes);
    }

    public static VectorFloat<?> toVectorFloat(float... coordinates) {
        return ArrayVectorProvider.getInstance().createFloatVector(coordinates);
    }
}
