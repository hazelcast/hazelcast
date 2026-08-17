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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier;
import com.hazelcast.internal.metrics.impl.MetricDescriptorImpl;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.internal.impl.VectorTestUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PUT_ALL_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_INDEX_QUERY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_INDEX_VISITED_NODES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_SEARCH_RESULTS_COUNT;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static com.hazelcast.vector.internal.impl.proxy.VectorCollectionProxyTest.DOC_1D_NEGATIVE;
import static com.hazelcast.vector.internal.impl.proxy.VectorCollectionProxyTest.DOC_1D_POSITIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class VectorCollectionOperationMetricsTest extends VectorCollectionMetricsTestBase {

    @Test
    public void getEmptyStats() {
        getVectorCollectionWith1Dim(operationInstance());

        for (var operation : List.of("get", "put", "putAll", "set", "remove", "delete", "search", "optimize", "size", "clear")) {
            assertOperationStats(statsInstance(), operation, 0);
        }
    }

    @Test
    public void lastAccessTime() {
        long startTime = System.currentTimeMillis();
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.getAsync("1")).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertStats(statsInstance(), captures -> {
            assertMetric(captures, VECTOR_COLLECTION_LAST_ACCESS_TIME,
                    value -> assertThat(value).isGreaterThanOrEqualTo(startTime));
            assertMetricEqual(captures, VECTOR_COLLECTION_LAST_UPDATE_TIME, 0);
        });
    }

    @Test
    public void lastUpdateTime() {
        long startTime = System.currentTimeMillis();
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertStats(statsInstance(), captures -> {
            assertMetric(captures, VECTOR_COLLECTION_LAST_ACCESS_TIME,
                    value -> assertThat(value).isGreaterThanOrEqualTo(startTime));
            assertMetric(captures, VECTOR_COLLECTION_LAST_UPDATE_TIME,
                    value -> assertThat(value).isGreaterThanOrEqualTo(startTime));
        });
    }

    @Test
    public void get() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.getAsync("1")).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "get", 1);
    }

    @Test
    public void put() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "put", 1);
    }

    @Test
    public void set() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.setAsync("1", DOC_1D_POSITIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "set", 1);
    }

    @Test
    public void putIfAbsent() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_POSITIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "put", 1);
    }

    @Test
    public void putAllSingleEntryViaMember() {
        assumeThat(operationSource).isNotEqualTo(OperationSource.CLIENT);

        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.putAllAsync(Map.of("1", DOC_1D_POSITIVE))).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "putAll", 1, (captures, baseDescriptor) ->
                assertThat(captures)
                        .hasEntrySatisfying(baseDescriptor.withMetric("putAllEntryCount").withUnit(ProbeUnit.COUNT),
                                value -> assertThat(value.singleCapturedValue().longValue()).isEqualTo(1)));
    }

    @Test
    public void putAllSingleEntryViaClient() {
        assumeThat(operationSource).isEqualTo(OperationSource.CLIENT);
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        // there is optimization for 1-entry partition, it is sent as Set not PutAll.
        assertThat(vectorCollection.putAllAsync(Map.of("1", DOC_1D_POSITIVE))).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "set", 1);
    }

    @Test
    public void putAllStatsManyEntries() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.putAllAsync(Map.of(
                // generate keys in the same partition to avoid converting putAll to set
                generateKeyForPartition(member, 0), DOC_1D_POSITIVE,
                generateKeyForPartition(member, 0), DOC_1D_NEGATIVE)
        )).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "putAll", 1, (captures, baseDescriptor) ->
                assertThat(captures)
                        .hasEntrySatisfying(baseDescriptor.withMetric(VECTOR_COLLECTION_PUT_ALL_ENTRY_COUNT).withUnit(ProbeUnit.COUNT),
                                value -> assertThat(value.singleCapturedValue().longValue()).isEqualTo(2)));
    }

    @Test
    public void remove() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.removeAsync("1")).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "remove", 1);
    }

    @Test
    public void delete() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "delete", 1);
    }

    @Test
    public void optimize() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.optimizeAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "optimize", 1);
    }

    @Test
    public void clear() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());

        assertThat(vectorCollection.clearAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "clear", 1);
    }

    @Test
    public void size() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());
        warmupOneIndexCollection(member, vectorCollection);

        vectorCollection.size();

        assertOperationStats(statsInstance(), "size", 1);
    }

    @Test
    public void search() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());
        warmupOneIndexCollection(member, vectorCollection);
        warmupOneIndexCollection(member, vectorCollection);
        int partitionCount = member.getPartitionService().getPartitions().size();

        assertThat(vectorCollection.searchAsync(VectorTestUtils.randomVec(1),
                SearchOptions.builder().limit(10).build())).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "search", 1, (captures, baseDescriptor) -> {
            assertThat(captures).hasEntrySatisfying(
                    baseDescriptor.withMetric(VECTOR_COLLECTION_SEARCH_RESULTS_COUNT).withUnit(ProbeUnit.COUNT),
                    value -> assertThat(value.singleCapturedValue().longValue()).isEqualTo(10));

            // lite member does not have vector indexes
            if (operationSource != OperationSource.LITE_MEMBER) {
                assertThat(captures).hasEntrySatisfying(
                        baseDescriptor.withMetric(VECTOR_COLLECTION_SEARCH_INDEX_QUERY_COUNT).withUnit(ProbeUnit.COUNT),
                        value -> assertThat(value.singleCapturedValue().longValue()).isEqualTo(partitionCount));
                assertThat(captures).hasEntrySatisfying(
                        baseDescriptor.withMetric(VECTOR_COLLECTION_SEARCH_INDEX_VISITED_NODES).withUnit(ProbeUnit.COUNT),
                        value -> assertThat(value.singleCapturedValue().longValue()).isEqualTo(2L * partitionCount));
            }
        });
    }

    @Test
    public void searchLessEntriesThanRequested() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());
        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        assertThat(vectorCollection.putAsync("2", DOC_1D_NEGATIVE)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertThat(vectorCollection.searchAsync(VectorTestUtils.randomVec(1),
                SearchOptions.builder().limit(10).build())).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        assertOperationStats(statsInstance(), "search", 1, (captures, baseDescriptor) ->
                assertThat(captures).hasEntrySatisfying(
                        baseDescriptor.withMetric(VECTOR_COLLECTION_SEARCH_RESULTS_COUNT).withUnit(ProbeUnit.COUNT),
                        value -> assertThat(value.singleCapturedValue().longValue())
                                .as("Should measure actually returned results")
                                .isEqualTo(2)));
    }

    @Test
    public void destroy() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());
        warmupOneIndexCollection(member, vectorCollection);

        vectorCollection.destroy();

        assertStats(statsInstance(), captures -> assertThat(captures)
                .as("There should be no metrics after collection is destroyed")
                .isEmpty());
    }

    protected void assertOperationStats(HazelcastInstance instance, String operationName, long expectedCount) {
        assertOperationStats(instance, operationName, expectedCount, (c, m) -> {});
    }

    protected void assertOperationStats(HazelcastInstance instance, String operationName, long expectedCount,
                                        BiConsumer<Map<MetricDescriptor, CapturingCollector.Capture>,
                                                MetricDescriptor> extraAssertions) {
        assertStats(instance, captures -> {
            var baseDescriptor = getBaseDescriptor();

            assertThat(captures).hasEntrySatisfying(
                    baseDescriptor.withMetric(operationName + "Count").withUnit(ProbeUnit.COUNT),
                    value -> assertThat(value.singleCapturedValue()).isEqualTo(expectedCount));

            // note that if the operation takes less that 1ms, the metric will still be non-zero
            // see "rounding up" in convertNanosToMillis
            assertThat(captures).hasEntrySatisfying(
                    baseDescriptor.withMetric("total" + capitalize(operationName) + "Latency").withUnit(ProbeUnit.MS),
                    value -> {
                        if (expectedCount > 0) {
                            assertThat(value.singleCapturedValue().longValue()).isPositive();
                        } else {
                            assertThat(value.singleCapturedValue().longValue()).isZero();

                        }
                    });

            extraAssertions.accept(captures, baseDescriptor);
        });
    }

    private MetricDescriptorImpl getBaseDescriptor() {
        return DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER.get()
                .withDiscriminator(VECTOR_COLLECTION_DISCRIMINATOR_NAME, collectionName)
                .withPrefix(VECTOR_COLLECTION_PREFIX);
    }

    private static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
