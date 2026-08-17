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

package com.hazelcast.vector.internal.impl.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Pipelining;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.internal.util.JVMUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_HEAP_COST;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * Checks that the formula in capacity planning documentation holds with reasonable precision
 * for various setups.
 */
// some tests allocate significant amount of memory and the tests generally consume a lot of CPU
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class})
public class VectorCollectionCapacityPlanningMetricsTest extends VectorCollectionMetricsTestBase {

    @Parameterized.Parameter(1)
    public int clusterSize;

    @Parameterized.Parameter(2)
    public int entryCount;

    @Parameterized.Parameter(3)
    public int keySize;

    @Parameterized.Parameter(4)
    public int valueSize;

    @Parameterized.Parameter(5)
    public int dimensions;

    @Parameterized.Parameter(6)
    public int maxDegree;

    @Parameterized.Parameter(7)
    public boolean useDeduplication;

    @Parameterized.Parameter(8)
    public int backupCount;

    @Override
    @Nonnull
    protected Config getConfig() {
        // regular instance is faster when adding entries because it has more partition threads
        Config config = regularInstanceConfig();
        config.getJetConfig().setEnabled(false);
        return config;
    }

    @Parameterized.Parameters(name = "clusterSize={1},entryCount={2},keySize={3},valueSize={4},dimensions={5},maxDegree={6},dedup={7},backupCount={8}")
    public static List<Object[]> parameters() {
        var basicCases = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(10_000, 100_000),
                // payloads: only id, XS, S, L
                keySizes(), valueSizes(),
                // dimensions - high numbers require more computation, check some values only
                dimensions(),
                maxDegrees(),
                useDeduplication(),
                noBackups()
        );

        var backups = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(100_000),
                // payloads: only id, XS, S, L
                keySizes(), valueSizes(),
                dimensionsSubset(),
                defaultMaxDegree(),
                useDeduplication(),
                // backups
                List.of(0, 1, 2));

        var noDeduplication = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(100_000),
                // payloads: only id, XS, S, L
                keySizes(), valueSizes(),
                dimensionsSubset(),
                defaultMaxDegree(),
                // deduplication
                List.of(false),
                noBackups());

        var smallCluster = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(1),
                // entry count
                List.of(10_000),
                // payloads: only id, XS, S, L
                keySizes(), valueSizes(),
                dimensionsSubset(),
                defaultMaxDegree(),
                useDeduplication(),
                noBackups());

        var bigKey = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(10_000),
                // payloads: only id, XS, S, L
                List.of(100, 1024), valueSizes(),
                // dimension
                List.of(10),
                defaultMaxDegree(),
                useDeduplication(),
                noBackups());

        var bigVector = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(10_000),
                // payloads: only id, XS, S, L
                keySizes(), valueSizes(),
                // dimension like in dbpedia benchmark
                List.of(1536),
                defaultMaxDegree(),
                useDeduplication(),
                noBackups());

        var bigCollection = cartesianProduct(List.of(OperationSource.MEMBER),
                // cluster size
                List.of(3),
                // entry count
                List.of(1_000_000),
                // payloads: only id, XS, S. do not use L to avoid excessive memory usage
                keySizes(), List.of(4, 100, 1024),
                // dimension like in deep-image-96-angular benchmark. small dimension to make it faster
                List.of(96),
                defaultMaxDegree(),
                useDeduplication(),
                noBackups());

        List<Object[]> parameters = new ArrayList<>();
        parameters.addAll(basicCases);
        parameters.addAll(backups);
        parameters.addAll(noDeduplication);
        parameters.addAll(smallCluster);
        parameters.addAll(bigKey);
        parameters.addAll(bigVector);
        parameters.addAll(bigCollection);
        return parameters;
    }

    @Nonnull
    private static List<Integer> noBackups() {
        return List.of(0);
    }

    @Nonnull
    private static List<Boolean> useDeduplication() {
        return List.of(true);
    }

    @Nonnull
    private static List<Integer> defaultMaxDegree() {
        return List.of(VectorIndexConfig.DEFAULT_MAX_DEGREE);
    }

    @Nonnull
    private static List<Integer> maxDegrees() {
        return List.of(VectorIndexConfig.DEFAULT_MAX_DEGREE, 2 * VectorIndexConfig.DEFAULT_MAX_DEGREE);
    }

    @Nonnull
    private static List<Integer> dimensions() {
        return List.of(1, 10, 384);
    }

    @Nonnull
    private static List<Integer> dimensionsSubset() {
        return List.of(10, 384);
    }

    @Nonnull
    private static List<Integer> valueSizes() {
        return List.of(4, 100, 1024, 10240);
    }

    @Nonnull
    private static List<Integer> keySizes() {
        // by default testing only with one, small key size to decrease number of combinations
        // this is based on the assumption that key and value storage needs are simply added
        // (see BinaryMapEntryCostEstimator.calculateEntryCost) and that the reference to key Data
        // is reused in all maps (this might not be true in the future in case of HD).
        return List.of(4);
    }

    @Override
    protected int getClusterSize() {
        return clusterSize;
    }

    private VectorCollection<byte[], byte[]> getVectorCollection() {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setMaxDegree(maxDegree)
                // note that the vectors are not normalized, dot metric should not be used
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(dimensions)
                .setUseDeduplication(useDeduplication);
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .setBackupCount(backupCount)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }

    @BeforeClass
    public static void beforeClass() {
        // print header for easier grepping the logs and use as CSV
        System.out.println("heapcost,clusterSize, backupCount, actualEntryCount, keySize + valueSize, "
                + "dimensions, maxDegree, useDeduplication,"
                + "totalHeapCost, estimatedMemoryFootprint, difference, simplifiedEstimatedMemoryFootprint, simplifiedDifference");
        System.out.flush();
    }

    @Test
    public void capacityPlanningFormulaIsAccurateEnough() throws InterruptedException, ExecutionException {
        Assume.assumeTrue("Formula is accurate only with Compressed OOPs.", JVMUtil.isCompressedOops());

        var collection = getVectorCollection();

        var start = Timer.nanos();
        Pipelining<Void> pipelining = new Pipelining<>(10);
        for (int i = 0; i < entryCount; ) {
            Map<byte[], VectorDocument<byte[]>> batch = new HashMap<>();
            for (int j = 0; j < 1_000 && i < entryCount; ++i, ++j) {
                batch.put(randomKey(), randomValue());
            }
            pipelining.add(collection.putAllAsync(batch));
        }
        pipelining.results();
        System.out.println("Data ingestion took " + Timer.millisElapsed(start) + " ms");

        // with small keys there can be duplicate keys (birthday paradox), use actual size for calculation
        var actualEntryCount = collection.size();

        // this estimation is with deduplication enabled. without deduplication the size should be slightly smaller
        // (unless there are many duplicates)
        // there will be at most clusterSize backups, even if more are configured
        long estimatedMemoryFootprint = (long) (Math.min(clusterSize, 1 + backupCount) * actualEntryCount
                // note: the general overhead depends on architecture (32/64 bit) and if compressed oops are enabled
                // value 308 was computed on OpenJDK 21 on aarch64 with compressed oops enabled (default) and default alignment (8 bytes)
                * (308 + keySize + valueSize
                        // vectors
                        + dimensions * 4L
                        // vector index neighbour arrays
                        // 1.2 - overflow, there is also extra entry in the neighbours array, but it is included in general overhead
                        // note that in general overhead only 1 vector index overhead is included
                        + maxDegree * 1.2 * 8));

        boolean simplifiedEstimationApplies = keySize + valueSize + dimensions * 4L >= 6000;
        long rawDataSize = keySize + valueSize + dimensions * 4L;
        long simplifiedEstimatedMemoryFootprint = simplifiedEstimationApplies
                ? (long) (Math.min(clusterSize, 1 + backupCount) * actualEntryCount * rawDataSize * 1.1)
                : -1;

        long totalHeapCost = getTotalHeapCost();

        System.out.printf("heapcost,%d,%d,%d,%d,%d,%d,%b,%d,%d,%.2f,%d,%.2f\n",
                clusterSize, backupCount, actualEntryCount, keySize + valueSize,
                dimensions, maxDegree, useDeduplication,
                totalHeapCost, estimatedMemoryFootprint, 100.0 * (totalHeapCost - estimatedMemoryFootprint) / totalHeapCost,
                simplifiedEstimatedMemoryFootprint, 100.0 * (totalHeapCost - simplifiedEstimatedMemoryFootprint) / totalHeapCost);

        // if these assertions fail it can be caused by:
        // 1. the formula became outdated
        // 2. different JVM setup (compressed OOPS/pointer size, alignment, which are also affected by heap size)
        // 3. if it only fails for smaller entry sizes it is likely that the per-entry constant overhead is different
        assertThat(totalHeapCost).as("The estimation should be accurate before optimization")
                .isCloseTo(estimatedMemoryFootprint, withinPercentage(10));

        if (simplifiedEstimationApplies) {
            assertThat(totalHeapCost).as("The simplified estimation should be accurate before optimization")
                    .isCloseTo(simplifiedEstimatedMemoryFootprint, withinPercentage(10));
        }

        collection.optimizeAsync().toCompletableFuture().join();

        // get updated value
        long totalHeapCostAfterOptimize = getTotalHeapCost();
        assertThat(totalHeapCostAfterOptimize).as("The estimation should be accurate after optimization")
                .isCloseTo(estimatedMemoryFootprint, withinPercentage(10));

        if (simplifiedEstimationApplies) {
            assertThat(totalHeapCostAfterOptimize).as("The simplified estimation should be accurate after optimization")
                    .isCloseTo(simplifiedEstimatedMemoryFootprint, withinPercentage(10));
        }

    }

    private byte[] randomKey() {
        byte[] bytes = new byte[keySize];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private VectorDocument<byte[]> randomValue() {
        byte[] bytes = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(bytes);
        return VectorDocument.of(bytes, randomVec(dimensions));
    }

    private long getTotalHeapCost() {
        // note that heap cost reported by vector collection metrics is lower that the actual cost
        // as some overheads are not included.
        AtomicLong heapMemoryCost = new AtomicLong();
        factory.getAllHazelcastInstances().forEach(instance -> {
            assertStats(instance, captures -> {
                assertMetric(captures, VECTOR_COLLECTION_HEAP_COST, heapMemoryCost::getAndAdd);
            });
        });
        return heapMemoryCost.get();
    }
}
