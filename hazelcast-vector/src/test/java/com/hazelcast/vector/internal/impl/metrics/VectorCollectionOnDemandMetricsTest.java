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

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.internal.impl.stats.LocalVectorCollectionStatsProvider;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_HEAP_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_OWNED_ENTRY_HEAP_MEMORY_COST;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static org.assertj.core.api.Assertions.assertThat;

public class VectorCollectionOnDemandMetricsTest extends VectorCollectionMetricsTestBase {

    @Parameterized.Parameter(1)
    public int clusterSize;

    @Parameterized.Parameters(name = "operationSource={0},clusterSize={1}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(OperationSource.values()), List.of(1, 2));
    }

    @Override
    protected int getClusterSize() {
        return clusterSize;
    }

    @Test
    public void emptyCollection() {
        long startTime = System.currentTimeMillis();

        getVectorCollectionWith1Dim(operationInstance());

        assertStats(statsInstance(), captures -> {
            assertMetric(captures, VECTOR_COLLECTION_CREATION_TIME,
                    value -> assertThat(value).isGreaterThanOrEqualTo(startTime));

            // total cost includes cost of stats
            assertMetricEqual(captures, VECTOR_COLLECTION_HEAP_COST,
                    LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED);

            assertMetricEqual(captures, VECTOR_COLLECTION_OWNED_ENTRY_COUNT, 0);
            assertMetricEqual(captures, VECTOR_COLLECTION_OWNED_ENTRY_HEAP_MEMORY_COST, 0);
            assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_COUNT, VectorCollectionConfig.DEFAULT_BACKUP_COUNT);
            assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_ENTRY_COUNT, 0);
            assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST, 0);

            assertMetricEqual(captures, VECTOR_COLLECTION_LAST_ACCESS_TIME, 0);
            assertMetricEqual(captures, VECTOR_COLLECTION_LAST_UPDATE_TIME, 0);
        });
    }

    @Test
    public void oneEntryInEachPartition() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(operationInstance());
        warmupOneIndexCollection(member, vectorCollection);
        int partitionCount = getPartitionService(member).getPartitionCount();
        int ownedPartitionCount = getPartitionService(member).getMemberPartitions(getAddress(statsInstance())).size();

        assertStats(statsInstance(), captures -> {
            assertMetric(captures, VECTOR_COLLECTION_HEAP_COST,
                    operationSource == OperationSource.LITE_MEMBER
                    // lite member has only stats, data member also data
                     ? v -> assertThat(v).isEqualTo(LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED)
                     : v -> assertThat(v).isGreaterThan(LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED));

            assertMetricEqual(captures, VECTOR_COLLECTION_OWNED_ENTRY_COUNT, ownedPartitionCount);

            assertMetric(captures, VECTOR_COLLECTION_OWNED_ENTRY_HEAP_MEMORY_COST,
                    operationSource == OperationSource.LITE_MEMBER
                            // lite member has only stats, data member also data
                            ? v -> assertThat(v).isZero()
                            : v -> assertThat(v).isGreaterThan(LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED));

            assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_COUNT, VectorCollectionConfig.DEFAULT_BACKUP_COUNT);

            if (clusterSize == 1) {
                // with single member there are no backup replicas
                assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_ENTRY_COUNT, 0);
                assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST, 0);
            } else {
                assertMetricEqual(captures, VECTOR_COLLECTION_BACKUP_ENTRY_COUNT,
                        operationSource == OperationSource.LITE_MEMBER
                                ? 0
                                : partitionCount - ownedPartitionCount);
                assertMetric(captures, VECTOR_COLLECTION_BACKUP_ENTRY_HEAP_MEMORY_COST,
                        operationSource == OperationSource.LITE_MEMBER
                                // lite member has only stats, data member also data
                                ? v -> assertThat(v).isZero()
                                : v -> assertThat(v).isGreaterThan(LocalVectorCollectionStatsProvider.ENTRY_HEAP_BYTES_USED));
            }
        });
    }
}
