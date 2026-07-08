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

package com.hazelcast.vector.internal.impl.migration;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.Hints;
import com.hazelcast.vector.internal.impl.SearchResultImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.toMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query vector collection while members of the cluster are being shutdown and started
 * - variant with all partitions being the same and checking that all are being queried.
 * In this setup is it impossible to test updates.
 * <p>
 * This scenario fetches entire collection using similarity search which is slow and
 * unrealistic, but tests some edge cases.
 */
public class VectorCollectionSearchAllPartitionsFullCollectionBounceTest extends VectorCollectionSearchBounceTestBase {

    @Parameterized.Parameter(1)
    public boolean singleStage = false;

    @Parameterized.Parameters(name = "deduplication={0} singleStage={1}")
    public static List<Object[]> parameters() {
        var params = new ArrayList<>(cartesianProduct(List.of(false, true), List.of(false)));
        params.add(new Object[]{false, true});
        return params;
    }

    private final Map<String, Set<String>> expectedPartitionKeys = new HashMap<>();

    @Before
    public void prepareCollectionIdenticalPartitions() throws Exception {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(COLLECTION_NAME)
                .addVectorIndexConfig(new VectorIndexConfig().setMetric(Metric.EUCLIDEAN).setDimension(DIMENSION)
                        .setUseDeduplication(deduplicate));
        steadyMember.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<String, String> collection = steadyMember.getVectorCollection(vectorCollectionConfig.getName());

        // this should to be done before the bouncing starts,
        // even without migrations it takes significant time
        var load = new Pipelining<Void>(100);

        // each partition contains the same entries, allowing us to verify if all partitions remain consistent after migration.
        for (int i = 0; i < INITIAL_PARTITION_SIZE; ++i) {
            var newVec = generateVector();
            for (int p = 0; p < PARTITION_COUNT; ++p) {
                String key = generateKeyForPartition(steadyMember, p);
                expectedPartitionKeys.computeIfAbsent("" + p, k -> new HashSet<>()).add(key);
                load.add(collection.putAsync(key, VectorDocument.of("" + p, newVec))
                        .thenAcceptAsync(prev -> assertThat(prev).isNull(), CALLER_RUNS));
            }
        }
        load.results();

        // optimization affects search results, do that before first migration so all partitions are always the same
        collection.optimizeAsync().toCompletableFuture().get();

        assertThat(collection.size()).isEqualTo(INITIAL_PARTITION_SIZE * PARTITION_COUNT);
        System.out.println("Created initial collection");
    }

    @Test
    public void testQuery() {
        var testTasks = prepareQueryTasks();
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
    }

    private Runnable[] prepareQueryTasks() {
        Runnable[] testTasks = new Runnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        return testTasks;
    }

    public class QueryRunnable implements RunnableEx {

        private final VectorCollection<String, String> collection;

        public QueryRunnable(HazelcastInstance hazelcastInstance) {
            this.collection = hazelcastInstance.getVectorCollection(COLLECTION_NAME);
        }

        @Override
        public void runEx() throws Exception {
            assertThat(collection.size()).isEqualTo(INITIAL_SIZE / PARTITION_COUNT * PARTITION_COUNT);

            // retrieve all values from the collection
            final int limit = (int) collection.size();
            final SearchOptions opts = SearchOptions.builder()
                    .limit(limit)
                    .includeValue()
                    .includeVectors()
                    .hint(Hints.FORCE_SINGLE_STAGE_SEARCH, singleStage)
                    .build();

            var result = collection.searchAsync(randomVec(DIMENSION), opts)
                    .toCompletableFuture().get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            assertThat(result.size()).isEqualTo(limit);
            var resultsMap = toMap(result);
            assertThat(resultsMap).as("There should be no duplicates").hasSize(limit);

            // group the found vectors by partition
            var vectorsByPartitionResult = resultsMap.values().stream()
                    .map(v -> ((SearchResultImpl<?, ?>) v))
                    .collect(
                            groupingBy(
                                    SearchResultImpl::getValue,
                                    Collectors.mapping(r -> ((VectorValues.SingleVectorValues) r.getVectors()).vector(), Collectors.toSet())
                            )
                    );

            var keysByPartitionResult = resultsMap.values().stream()
                    .map(v -> ((SearchResultImpl<?, ?>) v))
                    .collect(
                            groupingBy(
                                    SearchResultImpl::getValue,
                                    Collectors.mapping(SearchResultImpl::getKey, Collectors.toSet())
                            )
                    );

            // verify that all partitions contain the same vectors.
            range(1, PARTITION_COUNT).forEach(
                    partition -> assertThat(vectorsByPartitionResult.get(partition))
                            .as("Partition %s is different", partition)
                            .isEqualTo(vectorsByPartitionResult.get(0))
            );

            // verify that all partitions contain expected keys.
            assertThat(keysByPartitionResult).isEqualTo(expectedPartitionKeys);
        }
    }

}
