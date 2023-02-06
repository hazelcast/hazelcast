/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.util.concurrent.BusySpinIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({NightlyTest.class})
public class PartitionsPredicatePerformanceTest extends HazelcastTestSupport {
    private static final int PARTITION_COUNT = 16;

    @Test(timeout = Long.MAX_VALUE)
    @Ignore
    public void aggregate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        int[] partitionsToAggregate = new int[] { 1, 2, 4, 8, 16 };
        int[] itemsPerPartition = new int[] { 1 };
        long[] predicateExecutionTimeNanos = new long[] { 1, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1), SECONDS.toNanos(1) };
        Map<String, Map<String, Double>> allTrials = new HashMap<>();
        for (int partitions : partitionsToAggregate) {
            for (int items : itemsPerPartition) {
                for (long nanos : predicateExecutionTimeNanos) {
                    StringJoiner joiner = new StringJoiner("|");
                    joiner.add(partitions + "");
                    joiner.add(items + "");
                    joiner.add((nanos / (double) SECONDS.toNanos(1)) * items + "");
                    allTrials.put(joiner.toString(), invokeTrial(nodeFactory, partitions, items, nanos));
                    System.out.println("Trial Complete");
                }
            }
        }
        System.out.println("Partitions Hit By Predicate | Items Per Partition | Predicate Load Factor (item seconds) | min (ms) | max (ms) | mean (ms)");
        System.out.println("| --- | --- | --- | --- | --- | --- |");
        allTrials.forEach((trial, results) -> {
            System.out.print(trial);
            System.out.print("|" + results.get("min"));
            System.out.print("|" + results.get("max"));
            System.out.print("|" + results.get("mean"));
            System.out.println();
        });
    }

    private Map<String, Double> invokeTrial(TestHazelcastInstanceFactory nodeFactory, int partitionsToAggregate, int itemsPerPartition, long predicateSleepForNanos) {
        Map<String, Double> results = new HashMap<>();
        createCluster(nodeFactory, PARTITION_COUNT, itemsPerPartition).forEach((hzInstance, hzMap) -> {
            try {
                Map<String, Integer> partitionIds = getPartitionIds(hzInstance, hzMap, partitionsToAggregate);
                Predicate<String, Integer> partitionAwarePredicate = new PartitionAwarePredicate(partitionIds, predicateSleepForNanos);
                // For a single partition, use the partitionPredicate constructor (to check for backwards compatible performance regression)
                Predicate<String, Integer> aggPredicate = partitionsToAggregate == 1
                                                                  ? Predicates.partitionPredicate(partitionIds.keySet().stream().findFirst().get(), partitionAwarePredicate)
                                                                  : Predicates.multiPartitionPredicate(partitionIds.keySet(), partitionAwarePredicate);
                long warmupEndTime = System.currentTimeMillis() + SECONDS.toMillis(10);
                while (System.currentTimeMillis() < warmupEndTime) {
                    measure(hzMap, aggPredicate);
                }
                long endTime = System.currentTimeMillis() + SECONDS.toMillis(60);
                double mean = measure(hzMap, aggPredicate);
                double min = mean;
                double max = mean;
                while (System.currentTimeMillis() < endTime) {
                    long result = measure(hzMap, aggPredicate);
                    if (min > result) {
                        min = result;
                    }
                    if (max < result) {
                        max = result;
                    }
                    mean = (mean + result) / 2.0;
                }
                results.put("mean", mean / (double) MILLISECONDS.toNanos(1));
                results.put("max", max / (double) MILLISECONDS.toNanos(1));
                results.put("min", min / (double) MILLISECONDS.toNanos(1));
            } finally {
                hzInstance.getCluster().shutdown();
            }
        });
        return results;
    }

    private long measure(IMap<String, Integer> hzMap, Predicate<String, Integer> aggPredicate) {
        long elapsed = System.nanoTime();
        Long aggregated = hzMap.aggregate(Aggregators.count(), aggPredicate);
        elapsed = System.nanoTime() - elapsed;
        return elapsed;
    }

    private Map<String, Integer> getPartitionIds(HazelcastInstance instance, IMap<String, Integer> hzMap, int count) {
        final PartitionService partitionService = instance.getPartitionService();
        return partitionService.getPartitions().stream().limit(count)
                       .collect(Collectors.toMap(
                               p -> hzMap.keySet().stream().filter(k -> partitionService.getPartition(k).getPartitionId() == p.getPartitionId()).findFirst().get(),
                               p -> p.getPartitionId()
                       ));
    }

    private Map<HazelcastInstance, IMap<String, Integer>> createCluster(TestHazelcastInstanceFactory nodeFactory, int partitionCount, int itemsPerPartition) {
        Config config = getConfig()
                                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + partitionCount);

        HazelcastInstance[] instances = IntStream.range(0, nodeFactory.getCount())
                                                .mapToObj(i -> nodeFactory.newHazelcastInstance(config))
                                                .toArray(HazelcastInstance[]::new);
        warmUpPartitions(instances);
        HazelcastInstance instance = instances[0];
        IMap<String, Integer> map = instance.getMap(randomString());
        for (int p = 0; p < partitionCount; p++) {
            for (int k = 0; k < itemsPerPartition; k++) {
                map.put(generateKeyForPartition(instance, p), k);
            }
        }
        return Collections.singletonMap(instance, map);
    }

    private static class PartitionAwarePredicate implements Predicate<String, Integer>, Serializable, HazelcastInstanceAware {
        private transient HazelcastInstance instance;
        private Map<String, Integer> partitions;
        private long spinWaitNanos;

        PartitionAwarePredicate(Map<String, Integer> partitions, long spinWaitNanos) {
            this.partitions = partitions;
            this.spinWaitNanos = spinWaitNanos;
        }

        @Override
        public boolean apply(Entry<String, Integer> mapEntry) {
            IdleStrategy idleStrategy = new BusySpinIdleStrategy();
            long deadLine = System.nanoTime() + spinWaitNanos;
            for (int i = 0; System.nanoTime() < deadLine; i++) {
                idleStrategy.idle(i);
            }
            boolean result = partitions.containsKey(mapEntry.getKey());
            return result;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }
}
