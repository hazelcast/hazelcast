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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.collection.ArrayUtils;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.VectorTestUtils.toMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query vector collection while members of the cluster are being shutdown and started
 * - variant with random partition contents and updates.
 * In this setup is it impossible to check if all partitions were queried.
 */
public class VectorCollectionSearchRandomBounceTest extends VectorCollectionSearchBounceTestBase {

    @Before
    public void prepareCollectionRandomPartitions() throws Exception {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(COLLECTION_NAME)
                .addVectorIndexConfig(new VectorIndexConfig().setMetric(Metric.EUCLIDEAN).setDimension(DIMENSION)
                        .setUseDeduplication(deduplicate));
        steadyMember.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<String, String> collection = steadyMember.getVectorCollection(vectorCollectionConfig.getName());

        // this should be done before the bouncing starts,
        // even without migrations it takes significant time
        var load = new Pipelining<VectorDocument<String>>(100);

        // each partition contains the same entries, so we can detect if all PARTITION_COUNT top results are the same
        for (int i = 0; i < INITIAL_SIZE ; ++i) {
            VectorValues newVec = generateVector();
            var key = "" + nextKey.getAndIncrement();
            load.add(collection.putAsync(key, VectorDocument.of(key, newVec)));
        }
        load.results();

        System.out.println("Created initial collection");
    }

    @Test
    public void testQuery() {
        Runnable[] queryTasks = prepareQueryTasks();
        bounceMemberRule.testRepeatedly(queryTasks, MINUTES.toSeconds(3));
    }

    @Test
    public void testQueryAndUpdates() {
        Runnable[] queryTasks = prepareQueryTasks();
        Runnable[] updateTasks = new UpdateRunnable[] { new UpdateRunnable(bounceMemberRule.getNextTestDriver()) };
        bounceMemberRule.testRepeatedly(ArrayUtils.append(queryTasks, updateTasks), MINUTES.toSeconds(3));
    }

    @Test
    public void testQueryAndConcurrentUpdates() {
        Runnable[] queryTasks = prepareQueryTasks();
        Runnable[] updateTasks = new UpdateRunnable[] { new UpdateRunnable(bounceMemberRule.getNextTestDriver()),
                new UpdateRunnable(bounceMemberRule.getNextTestDriver()),
                new UpdateRunnable(bounceMemberRule.getNextTestDriver())
        };
        bounceMemberRule.testRepeatedly(ArrayUtils.append(queryTasks, updateTasks), MINUTES.toSeconds(3));
    }

    private Runnable[] prepareQueryTasks() {
        Runnable[] testTasks = new Runnable[CONCURRENCY];
        for (int i = 0; i < CONCURRENCY; i++) {
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver());
        }
        return testTasks;
    }

    public static class QueryRunnable implements RunnableEx {
        private final VectorCollection<String, String> collection;

        public QueryRunnable(HazelcastInstance hazelcastInstance) {
            this.collection = hazelcastInstance.getVectorCollection(COLLECTION_NAME);
        }

        @Override
        public void runEx() throws Exception {
            assertThat(collection.size()).isEqualTo(INITIAL_SIZE);

            final int limit = ThreadLocalRandom.current().nextInt(1, 50);
            final SearchOptions opts = SearchOptions.builder()
                    .limit(limit)
                    .includeValue()
                    .includeVectors()
                    .build();

            long start = Timer.nanos();
            var result = collection.searchAsync(randomVec(DIMENSION), opts)
                    .toCompletableFuture().get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            long millis = Timer.millisElapsed(start);

            if (millis > 500) {
                // Unexpectedly slow search. Slowness may be caused by partition migrations
                // and partition table distribution delays, so is not directly a bug,
                // but may be important for understanding other undesirable behaviors.
                System.out.printf("[%s] search returned %d results in %d ms %n", Thread.currentThread().getName(),
                        result.size(), millis);
            }
            assertThat(result.size()).isEqualTo(limit);
            var resultsMap = toMap(result);
            assertThat(resultsMap).as("There should be no duplicates").hasSize(limit);
        }
    }

    public class UpdateRunnable implements RunnableEx {
        private final VectorCollection<String, String> collection;
        private int counter;

        public UpdateRunnable(HazelcastInstance hazelcastInstance) {
            this.collection = hazelcastInstance.getVectorCollection(COLLECTION_NAME);
        }

        @Override
        public void runEx() {
            String entryToUpdate = "" + ThreadLocalRandom.current().nextInt(nextKey.get());
            assertThat(collection.putAsync(entryToUpdate, VectorDocument.of(entryToUpdate, generateVector())))
                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                    .as("Entry %s should exist", entryToUpdate).isNotNull();

            if (++counter % 1_000 == 0) {
                // update is rather slow with bouncing members, as the partitions are often blocked for migration
                // which in turn requires optimization and takes non-negligible time
                System.out.println("Update progress: " + counter);
            }
        }
    }
}
