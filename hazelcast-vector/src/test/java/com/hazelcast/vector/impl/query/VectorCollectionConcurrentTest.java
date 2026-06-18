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

package com.hazelcast.vector.impl.query;

import com.google.common.collect.Lists;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.VectorTestUtils.toMap;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class VectorCollectionConcurrentTest extends HazelcastTestSupport {
    private static final int INITIAL_SIZE = 10_000;

    private final int concurrency = 10;
    private final int dimension = 100;
    public int clusterSize = 3;
    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance member;
    private HazelcastInstance client;
    private VectorCollection<Integer, String> collection;
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicLong searchCount = new AtomicLong();
    private final AtomicLong updateCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();
    private final AtomicLong cleanupCount = new AtomicLong();
    private final AtomicInteger nextKey = new AtomicInteger();

    @Parameterized.Parameters(name = "deduplicate={0}, useClient={1}")
    public static Object[] parameters() {
        return Lists.cartesianProduct(List.of(List.of(false, true), List.of(false, true)))
                .stream()
                .map(tuple -> tuple.toArray(new Object[0]))
                .toArray(Object[]::new);
    }

    @Parameterized.Parameter
    public boolean deduplicate;

    @Parameterized.Parameter(1)
    public boolean useClient;

    @Before
    public void setup() {
        members = factory.newInstances(smallInstanceConfigWithoutJetAndMetrics(), clusterSize);
        member = members[0];
        client = factory.newHazelcastClient();

        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig("col")
                .addVectorIndexConfig(new VectorIndexConfig().setMetric(Metric.EUCLIDEAN).setDimension(dimension)
                        .setUseDeduplication(deduplicate));
        hz().getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        collection = hz().getVectorCollection(vectorCollectionConfig.getName());
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    protected HazelcastInstance hz() {
        return useClient ? client : member;
    }

    CompletionStage<Object> search(int allowedFault) {
        final int limit = ThreadLocalRandom.current().nextInt(1, 50);
        final SearchOptions opts = SearchOptions.builder()
                .limit(limit)
                .includeValue().includeVectors()
                // below probabilities may result in a more realistic workload
                // (vectors should be requested rarely in practice), but they
                // make concurrent modification situation during search less likely.
//                .setIncludeValue(ThreadLocalRandom.current().nextDouble() > 0.5)
//                .setIncludeVectors(ThreadLocalRandom.current().nextDouble() > 0.9)
                .build();

        return collection.searchAsync(randomVec(dimension), opts).thenCompose(result -> {
            searchCount.incrementAndGet();
            // It is extremely unlikely to get less results due to concurrent updates,
            // but in theory this test is flaky. If this starts happening too often,
            // we would need to reassess the assumption.
            assertThat(result.size()).isBetween(limit - allowedFault, limit);
            var resultMap = toMap(result);
            assertThat(resultMap).as("Should return correct results with options %s", opts).allSatisfy((key, document) -> {
                if (opts.isIncludeValue()) {
                    assertThat(document.getValue()).as("Should return metadata").isEqualTo(key.toString());
                }
                if (opts.isIncludeVectors()) {
                    assertThat(((VectorValues.SingleVectorValues) document.getVectors()).vector()).as("Should return vector").hasSize(dimension);
                }
            });

            if (stop.get()) {
                System.out.println("testSearch done");
                return CompletableFuture.completedFuture(null);
            }
            return search(allowedFault);
        });
    }

    AtomicReference<VectorValues> recentVector = new AtomicReference<>(randomVec(dimension));

    private CompletionStage<VectorDocument<String>> putNewRandomEntry() {
        int key = nextKey.getAndIncrement();
        // the probabilities are relatively high, but they entries are distributed among partitions
        // (by default 11 in this test) so final duplicate vector probability is lower.
        boolean reuse = deduplicate && ThreadLocalRandom.current().nextDouble() > 0.5;
        boolean store = deduplicate && ThreadLocalRandom.current().nextDouble() > 0.5;
        var newVec = reuse ? recentVector.get() : randomVec(dimension);
        if (store && !reuse) {
            recentVector.set(newVec);
        }

        return collection.putAsync(key, VectorDocument.of("" + key, newVec));
    }

    CompletionStage<Object> insert() {
        return putNewRandomEntry().thenCompose(prev -> {
            assertThat(prev).isNull();
            if (stop.get()) {
                System.out.println("testInsert done");
                return CompletableFuture.completedFuture(null);
            }
            return insert();
        });
    }

    CompletionStage<Object> update() {
        // this entry might not exist if it was removed earlier - this is not a problem
        int entryToUpdate = ThreadLocalRandom.current().nextInt(nextKey.get());
        boolean reuse = deduplicate && ThreadLocalRandom.current().nextDouble() > 0.5;
        return collection.setAsync(entryToUpdate, VectorDocument.of("" + entryToUpdate,
                        reuse ? recentVector.get() : randomVec(dimension)))
                .thenCompose(v -> {
                    updateCount.incrementAndGet();
                    if (stop.get()) {
                        System.out.println("testUpdate done");
                        return CompletableFuture.completedFuture(null);
                    }
                    return update();
                });
    }

    CompletionStage<Object> delete() {
        // this entry might not exist if it was removed earlier - this is not a problem
        int entryToDelete = ThreadLocalRandom.current().nextInt(nextKey.get());
        return collection.removeAsync(entryToDelete)
                .thenCompose(v -> {
                    deleteCount.incrementAndGet();
                    if (stop.get()) {
                        System.out.println("testDelete done");
                        return CompletableFuture.completedFuture(null);
                    }
                    return delete();
                });
    }

    CompletionStage<Object> cleanup() {
        // run cleanup after every ~500 deleted records.
        if (deleteCount.get() / (cleanupCount.get() + 1) > 500) {
            cleanupCount.getAndIncrement();
            return collection.optimizeAsync()
                    .thenApply(t -> null)
                    .thenCompose(v -> {
                                if (stop.get()) {
                                    System.out.println("testOptimize done");
                                    return CompletableFuture.completedFuture(null);
                                }
                                return cleanup();
                            }
                    );
        } else {
            return CompletableFuture.supplyAsync(() -> {
                        sleepSeconds(1);
                        if (stop.get()) {
                            System.out.println("testOptimize done");
                            return CompletableFuture.completedFuture(null);
                        }
                        return cleanup();
                    }
            );
        }
    }

    private <T> CompletionStage<T> stopOnError(CompletionStage<T> stage) {
        return stage.exceptionallyCompose(t -> {
            // stop test and propagate exception
            stop.set(true);
            return CompletableFuture.failedFuture(t);
        });
    }

    @Test
    public void testConcurrentSearchesAndModifications() throws Exception {
        var load = new Pipelining<VectorDocument<String>>(16);
        for (int i = 0; i < INITIAL_SIZE; ++i) {
            load.add(putNewRandomEntry());
        }
        load.results();
        System.out.println("Created initial collection");

        var futures = new CompletableFuture[concurrency + 3];
        for (int i = 0; i < concurrency; i++) {
            futures[i] = stopOnError(search(0)).toCompletableFuture();
        }
        futures[concurrency] = stopOnError(insert()).toCompletableFuture();
        futures[concurrency + 1] = stopOnError(update()).toCompletableFuture();
        futures[concurrency + 2] = stopOnError(delete()).toCompletableFuture();

        CompletableFuture<Void> combinedSearchFuture = CompletableFuture.allOf(futures);

        sleepAndStop(stop, 60);

        assertThat(combinedSearchFuture).succeedsWithin(TIMEOUT);

        System.out.println("Executed " + searchCount.get() + " searches");
        System.out.println("Number of added items: " + nextKey.get());
        System.out.println("Number of updated items: " + updateCount.get());
        System.out.println("Number of deleted items: " + deleteCount.get());

        // sanity checks that the test did anything
        assertThat(searchCount).hasValueGreaterThan(1000);
        assertThat(nextKey).hasValueGreaterThan(INITIAL_SIZE + 100);
        assertThat(updateCount).hasValueGreaterThan(100);
        assertThat(deleteCount).hasValueGreaterThan(100);
    }

    @Test
    public void testConcurrentSearchesModificationsAndOptimization() throws Exception {
        var load = new Pipelining<VectorDocument<String>>(16);
        for (int i = 0; i < INITIAL_SIZE; ++i) {
            load.add(putNewRandomEntry());
        }
        load.results();
        System.out.println("Created initial collection");

        var futures = new CompletableFuture[concurrency + 3];
        for (int i = 0; i < concurrency; i++) {
            futures[i] = stopOnError(search(2)).toCompletableFuture();
        }
        futures[concurrency] = stopOnError(cleanup()).toCompletableFuture();
        // mutable operations may fail if the index is blocked; ignore this error.
        futures[concurrency + 1] = stopOnError(insert()).toCompletableFuture();
        futures[concurrency + 2] = stopOnError(delete()).toCompletableFuture();

        CompletableFuture<Void> combinedSearchFuture = CompletableFuture.allOf(futures);

        stopWhenCondition(
                stop,
                () -> searchCount.get() > 1000 && deleteCount.get() > 2000 && cleanupCount.get() > 3,
                Duration.ofMinutes(2)
        );

        assertThat(combinedSearchFuture).succeedsWithin(TIMEOUT);

        System.out.println("Executed " + searchCount.get() + " searches");
        System.out.println("Number of added items: " + nextKey.get());
        System.out.println("Number of deleted items: " + deleteCount.get());
        System.out.println("Number of cleanup operation: " + cleanupCount.get());

        // sanity checks that the test did anything
        assertThat(searchCount).hasValueGreaterThan(1000);
        assertThat(nextKey).hasValueGreaterThan(INITIAL_SIZE + 100);
        assertThat(deleteCount).hasValueGreaterThan(2000);
        assertThat(cleanupCount).hasValueGreaterThan(3);
    }

    private void stopWhenCondition(AtomicBoolean stop, Supplier<Boolean> condition, Duration timeout) {
        var timeoutEnd = System.currentTimeMillis() + timeout.toMillis();
        while (!stop.get() && !condition.get() && System.currentTimeMillis() < timeoutEnd) {
            sleepSeconds(1);
        }
        stop.set(true);

    }
}
