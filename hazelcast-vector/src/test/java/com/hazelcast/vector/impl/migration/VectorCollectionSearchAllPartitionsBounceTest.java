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
import com.hazelcast.internal.util.collection.ArrayUtils;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.Hints;
import com.hazelcast.vector.impl.VectorCollectionBackupAccessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.VectorTestUtils.toMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Query vector collection while members of the cluster are being shutdown and started
 * - variant with all partitions being the same and checking that all are being queried.
 */
public class VectorCollectionSearchAllPartitionsBounceTest extends VectorCollectionSearchBounceTestBase {

    private static final ILogger LOGGER = Logger.getLogger(VectorCollectionSearchAllPartitionsBounceTest.class);

    @Parameterized.Parameters(name = "")
    public static List<Object[]> parameters() {
        // Search assert does not work well with duplicates
        // (might not get results from all partitions if the duplicates are found),
        // so only use index with deduplication but no actual duplicates.
        var params = new ArrayList<Object[]>();
        params.add(new Object[]{false});
        return params;
    }

    public record CollectionParams(int backupCount, int asyncBackupCount, boolean deduplicate) {
        @Nonnull String collectionName() {
            return String.format("%s-%d-%d%s", COLLECTION_NAME, backupCount(), asyncBackupCount(),
                    deduplicate ? "-deduplicated" : "");
        }

        int totalBackupCount() {
            return backupCount + asyncBackupCount;
        }
    }

    public record SearchParams(boolean singleStage) {}

    protected List<CollectionParams> getCollectionParams() {
        List<CollectionParams> params = new ArrayList<>();
        for (var deduplicate : List.of(false, true)) {
            params.add(new CollectionParams(1, 0, deduplicate));
            if (!useTerminate()) {
                // for terminate tests use only collection with sync backups
                params.add(new CollectionParams(0, 1, deduplicate));
                params.add(new CollectionParams(0, 0, deduplicate));
            }
        }
        // with deduplication only to limit number of combinations
        params.add(new CollectionParams(3, 0, true));

        return params;
    }

    protected List<SearchParams> getSearchParams() {
        return List.of(new SearchParams(false), new SearchParams(true));
    }

    // map: key = collection name
    // set: each entry is for single vector that is added to all partitions
    // inner map: key=partition id, value=key for that that partition
    private final Map<String, Set<Map<String, String>>> sameVectorKeys = new HashMap<>(3);

    @Before
    public void prepareCollectionIdenticalPartitions() throws Exception {
        List<CollectionParams> allParams = getCollectionParams();
        for (var params : allParams) {
            createCollection(params);
        }
        int totalReplicas = allParams.stream().mapToInt(p -> p.totalBackupCount() + 1).sum();
        System.out.println("Created " + allParams.size() + " initial collections with " + totalReplicas + " total replicas");
    }

    private void createCollection(CollectionParams params) throws Exception {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(params.collectionName())
                .setBackupCount(params.backupCount).setAsyncBackupCount(params.asyncBackupCount)
                .addVectorIndexConfig(new VectorIndexConfig()
                        .setMetric(Metric.EUCLIDEAN).setDimension(DIMENSION)
                        .setUseDeduplication(params.deduplicate));
        steadyMember.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<String, String> collection = steadyMember.getVectorCollection(vectorCollectionConfig.getName());

        // this should to be done before the bouncing starts,
        // even without migrations it takes significant time
        var load = new Pipelining<Void>(100);

        // each partition contains the same entries, allowing us to verify if all partitions remain consistent after migration.
        for (int i = 0; i < INITIAL_PARTITION_SIZE; ++i) {
            addVectorToAllPartitions(load, collection);
        }
        load.results();

        // optimization affects search results, do that before first migration so all partitions are as similar as possible
        assertThat(collection.optimizeAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertThat(collection.size()).isEqualTo(INITIAL_PARTITION_SIZE * PARTITION_COUNT);
    }

    private void addVectorToAllPartitions(Pipelining<Void> load, VectorCollection<String, String> collection) throws InterruptedException {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        var newVec = generateVector();
        Map<String, String> keys = new HashMap<>();
        for (int p = 0; p < PARTITION_COUNT; ++p) {
            String key = generateKeyForPartition(steadyMember, p);
            keys.put("" + p, key);
            load.add(collection.putAsync(key, VectorDocument.of("" + p, newVec))
                    .thenAcceptAsync(prev -> assertThat(prev).isNull(), CALLER_RUNS));
        }
        sameVectorKeys.computeIfAbsent(collection.getName(), __ -> new HashSet<>()).add(keys);
    }

    private void assertAllReplicasAreTheSame() {
        waitClusterForSafeState(bounceMemberRule.getSteadyMember());

        // check only live members.
        // Bouncing could have finished when the member was down. In such case it will not be started again.
        HazelcastInstance[] liveMembers = Arrays.stream(bounceMemberRule.getMembersSnapshot())
                .filter(i -> i.getLifecycleService().isRunning())
                .toArray(HazelcastInstance[]::new);

        LOGGER.info("Live members after test: " + Arrays.toString(liveMembers));

        // there should be no operations parking forever
        assertWaitingOperationCountEventually(0, liveMembers);

        // all partitions and replicas of given partitions should be the same
        for (var params : getCollectionParams()) {
            if (params.totalBackupCount() == 0) {
                // no replicas to compare
                continue;
            }

            int maxReplicaIndex = Math.min(liveMembers.length - 1, params.totalBackupCount());
            var accessors = IntStream.rangeClosed(0, maxReplicaIndex)
                    .mapToObj(index -> new VectorCollectionBackupAccessor<String, String>(
                            liveMembers, params.collectionName(), index))
                    .toList();
            var owner = accessors.get(0);
            var backups = accessors.subList(1, accessors.size());

            var allCollectionKeys = sameVectorKeys.get(params.collectionName()).stream()
                    .flatMap(m -> m.values().stream())
                    .toList();

            assertThat(accessors).allSatisfy(replica ->
                    assertThat(replica.size()).isEqualTo(INITIAL_PARTITION_SIZE * PARTITION_COUNT));

            // ensure that entries in all replicas are exactly the same
            // use owner data as ground truth.
            var ownerEntries = owner.getMandatoryEntries(allCollectionKeys);
            assertThat(backups).allSatisfy(replica ->
                    assertThat(replica.getMandatoryEntries(allCollectionKeys)).containsExactlyInAnyOrderEntriesOf(ownerEntries));
        }
    }

    @Test
    public void testQuery() {
        var testTasks = prepareQueryTasks();
        bounceMemberRule.testRepeatedly(testTasks, MINUTES.toSeconds(3));
        assertAllReplicasAreTheSame();
    }

    @Test
    public void testQueryAndUpdates() {
        Runnable[] queryTasks = prepareQueryTasks();
        Runnable[] updateTasks = prepareUpdateTasks(-1);
        bounceMemberRule.testRepeatedly(ArrayUtils.append(queryTasks, updateTasks), MINUTES.toSeconds(3));
        assertAllReplicasAreTheSame();
    }

    @Test
    public void testQueryAndUpdatesWithOptimize() {
        Runnable[] queryTasks = prepareQueryTasks();
        Runnable[] updateTasks = prepareUpdateTasks(1);
        bounceMemberRule.testRepeatedly(ArrayUtils.append(queryTasks, updateTasks), MINUTES.toSeconds(3));
        assertAllReplicasAreTheSame();
    }

    @Test(timeout = 15 * 60_000)
    public void testQueryAndUpdatesWithConcurrentOptimize() {
        Runnable[] queryTasks = prepareQueryTasks();
        Runnable[] updateTasks = prepareUpdateTasks(0);
        Runnable[] optimizeTasks = ArrayUtils.append(prepareOptimizeTasks(1000), prepareOptimizeTasks(1000));
        // some optimizations tend to take long (>2 minutes) so use longer test time to have more repeats
        bounceMemberRule.testRepeatedly(ArrayUtils.append(queryTasks, ArrayUtils.append(updateTasks, optimizeTasks)),
                MINUTES.toSeconds(6));
        assertAllReplicasAreTheSame();
    }

    @After
    public void dumpParkedOperations() {
        // it is ok if OptimizeBackupOperation and backups ops for async backups are still parked when the test ends.
        // other operations, in particular owner operations, should not be parked indefinitely.
        HazelcastInstance[] membersSnapshot = bounceMemberRule.getMembersSnapshot();
        dumpParkedOperations(membersSnapshot);
    }

    public static void dumpParkedOperations(HazelcastInstance... members) {
        for (var instance : members) {
            if (!instance.getLifecycleService().isRunning()) {
                continue;
            }

            final OperationParkerImpl waitNotifyService = getOperationParkingService(instance);
            LOGGER.info("Waiting ops for " + instance + ": "
                    + waitNotifyService.getTotalValidWaitingOperationCount() + "/" + waitNotifyService.getTotalParkedOperationCount());
            if (waitNotifyService.getTotalParkedOperationCount() > 0) {
                LOGGER.warning("Remaining waiting ops for " + instance + ": " + waitNotifyService.dump());
            }
        }
    }

    private Runnable[] prepareQueryTasks() {
        Runnable[] testTasks = new Runnable[CONCURRENCY];
        var allParams = cartesianProductTuple(getCollectionParams(), getSearchParams());
        for (int i = 0; i < CONCURRENCY; i++) {
            var params = allParams.get(i % allParams.size());
            testTasks[i] = new QueryRunnable(bounceMemberRule.getNextTestDriver(), params.f0(), params.f1());
        }
        return testTasks;
    }

    private Runnable[] prepareUpdateTasks(int optimizeFrequency) {
        return getCollectionParams().stream()
                .map(params -> new UpdateAndOptimizeRunnable(bounceMemberRule.getNextTestDriver(), optimizeFrequency, params))
                .toArray(Runnable[]::new);
    }

    private Runnable[] prepareOptimizeTasks(int optimizePeriod) {
        return getCollectionParams().stream()
                .map(params -> new OptimizeRunnable(bounceMemberRule.getNextTestDriver(), optimizePeriod, params))
                .toArray(Runnable[]::new);
    }

    public static class QueryRunnable implements RunnableEx {

        private final VectorCollection<String, String> collection;
        private final SearchParams searchParams;

        public QueryRunnable(HazelcastInstance hazelcastInstance, CollectionParams params, SearchParams searchParams) {
            this.collection = hazelcastInstance.getVectorCollection(params.collectionName());
            this.searchParams = searchParams;
        }

        @Override
        public void runEx() throws Exception {
            assertThat(collection.size())
                    .as("Collection %s should have correct size", collection.getName())
                    .isEqualTo(INITIAL_SIZE / PARTITION_COUNT * PARTITION_COUNT);

            // 3 * PARTITION_COUNT should make it improbable to get fewer partitions
            // because of the same distance between different vectors.
            //
            // Note: can it be flaky if graph entry point is removed during search?
            final int limit = ThreadLocalRandom.current().nextInt(3 * PARTITION_COUNT, 10 * PARTITION_COUNT);
            final SearchOptions opts = SearchOptions.builder()
                    .limit(limit)
                    .includeValue()
                    .includeVectors()
                    .hint(Hints.FORCE_SINGLE_STAGE_SEARCH, searchParams.singleStage)
                    .build();

            var result = collection.searchAsync(randomVec(DIMENSION), opts)
                    .toCompletableFuture().get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            assertThat(result.size()).isEqualTo(limit);
            var resultsMap = toMap(result);
            assertThat(resultsMap).as("There should be no duplicates").hasSize(limit);

            // group the found vectors by partition
            var vectorsByPartitionResult = resultsMap.values().stream()
                    .collect(
                            groupingBy(
                                    SearchResult::getValue,
                                    Collectors.mapping(SearchResult::getVectors, Collectors.toSet())
                            )
                    );
            assertThat(vectorsByPartitionResult.size())
                    .as("Should return data from all partitions for %s but got %s with limit %s",
                            collection.getName(), resultsMap, limit)
                    .isEqualTo(PARTITION_COUNT);
        }
    }

    /**
     * Updates all partitions in sync and optionally executes optimize
     * every given number of updates. Optimize and updates do not run in parallel.
     */
    public class UpdateAndOptimizeRunnable implements RunnableEx {
        private final VectorCollection<String, String> collection;
        // optimizeFrequency<=0 - optimize disabled
        private final int optimizeFrequency;
        private Iterator<Map<String, String>> entryIterator;
        private int counter;

        public UpdateAndOptimizeRunnable(HazelcastInstance hazelcastInstance, int optimizeFrequency, CollectionParams params) {
            this.collection = hazelcastInstance.getVectorCollection(params.collectionName());
            this.optimizeFrequency = optimizeFrequency;
            entryIterator = sameVectorKeys.get(collection.getName()).iterator();
        }

        @Override
        public void runEx() throws Exception {
            // update all partitions to new vector
            var load = new Pipelining<Void>(PARTITION_COUNT);
            var newVec = generateVector();

            if (!entryIterator.hasNext()) {
                // reset from the beginning
                entryIterator = sameVectorKeys.get(collection.getName()).iterator();
            }
            for (var partitionIdToKeyEntry : entryIterator.next().entrySet()) {
                load.add(collection.putAsync(partitionIdToKeyEntry.getValue(), VectorDocument.of(partitionIdToKeyEntry.getKey(), newVec))
                        .thenAcceptAsync(prev -> assertThat(prev)
                                .as("Previous value should exist in %s", collection.getName())
                                .isNotNull(), CALLER_RUNS));
            }
            assertThatNoException().isThrownBy(load::results);

            ++counter;

            if (optimizeFrequency > 0 && counter % optimizeFrequency == 0) {
                assertThat(collection.optimizeAsync())
                        .as("Should optimize %s", collection.getName())
                        .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
            }

            if (counter % 100 == 0) {
                // update is rather slow with bouncing members, as the partitions are often blocked for migration
                // which takes non-negligible time.
                // each iteration updates all partitions - report number of actual updates executed
                System.out.println("Update progress: " + counter * PARTITION_COUNT);
            }
        }
    }

    public static class OptimizeRunnable implements RunnableEx {
        private final VectorCollection<String, String> collection;
        private final int optimizePeriod;
        private int counter;

        public OptimizeRunnable(HazelcastInstance hazelcastInstance, int optimizePeriod, CollectionParams params) {
            this.collection = hazelcastInstance.getVectorCollection(params.collectionName());
            this.optimizePeriod = optimizePeriod;
        }

        @Override
        public void runEx() {
            if (counter == 0) {
                // randomize start
                sleepMillis(ThreadLocalRandom.current().nextInt(optimizePeriod));
            } else {
                sleepMillis(optimizePeriod);
            }
            ++counter;

            var start = System.currentTimeMillis();
            // note: optimize backups are async
            try {
                // Use longer than default timeout for optimisation.
                // There are many (unrealistically many) concurrent optimization requests, including multiple
                // requests for the same index to increase probability of finding race conditions and bugs.
                // If a request is unlucky it will be pushed from member to member due to migrations,
                // but will never reach the head of the queue. Once bouncing is stopped the requests execute to completion.
                // This is a kind of starvation, but on practice this should not be a problem.
                assertThat(collection.optimizeAsync())
                        .as("Should optimize %s", collection.getName())
                        .succeedsWithin(Duration.ofMinutes(10));
                LOGGER.info("Optimize took " + (System.currentTimeMillis() - start) + "ms on " + collection.getName());
            } catch (Throwable t) {
                // this could also be InterruptedException or AssertionError
                LOGGER.warning("Optimize failed after " + (System.currentTimeMillis() - start) + "ms on " + collection.getName(), t);
                throw t;
            }

            if (counter % 10 == 0) {
                System.out.println("Optimize progress: " + counter);
            }
        }
    }
}
