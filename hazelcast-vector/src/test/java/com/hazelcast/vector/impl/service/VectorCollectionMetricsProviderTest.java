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

package com.hazelcast.vector.impl.service;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.phonehome.MetricsCollectionContext;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.VectorPlatformUtil;
import com.hazelcast.vector.impl.ops.VectorOptimizeWaitNotifyKey;
import com.hazelcast.vector.impl.query.PartitionLimitEstimatingSearcher;
import com.hazelcast.vector.impl.query.SingleStageSearcher;
import com.hazelcast.vector.impl.query.TwoStageSearcher;
import com.hazelcast.vector.impl.stats.LocalVectorCollectionStatsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_COUNT;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_HEAP_USAGE;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_INDEX_COUNT;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_INDEX_DIMENSIONS;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS;
import static com.hazelcast.vector.impl.service.VectorCollectionPhoneHomeMetrics.VECTOR_COLLECTION_VECTOR_API_AVAILABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;

@ExtendWith(MockitoExtension.class)
class VectorCollectionMetricsProviderTest {

    private long calculateMinimumVectorServiceHeapUsage() {
        var partitionThreadCount = Accessors.getOperationService(member).getPartitionThreadCount();

        return VectorCollectionServiceImpl.FIXED_HEAP_BYTES_USED
                + PartitionLimitEstimatingSearcher.FIXED_HEAP_BYTES_USED + SingleStageSearcher.FIXED_HEAP_BYTES_USED
                + PartitionLimitEstimatingSearcher.FIXED_HEAP_BYTES_USED + TwoStageSearcher.FIXED_HEAP_BYTES_USED
                + LocalVectorCollectionStatsProvider.FIXED_HEAP_BYTES_USED
                // optimization service
                + VectorCollectionOptimizationManagerImpl.FIXED_HEAP_BYTES_USED
                + partitionThreadCount * VectorOptimizeWaitNotifyKey.FIXED_HEAP_BYTES_USED;
    }

    @Spy
    MetricsCollectionContext metricsCollectionContext;
    VectorCollectionMetricsProvider vectorCollectionMetricsProvider = new VectorCollectionMetricsProvider();
    TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    HazelcastInstance member;

    @AfterEach
    void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    void testPhoneHome_whenNoVectorCollectionConfigured() {
        member = hazelcastFactory.newHazelcastInstance();
        vectorCollectionMetricsProvider.provideMetrics(Accessors.getNode(member), metricsCollectionContext);
        InOrder inOrder = verifyMetrics(0, 0, false);
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_HEAP_USAGE, calculateMinimumVectorServiceHeapUsage());
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_VECTOR_API_AVAILABLE, VectorPlatformUtil.isVectorModulePresent());
    }

    @Test
    void testPhoneHome_whenVectorCollectionConfigured() {
        givenVectorCollectionsConfigured();
        vectorCollectionMetricsProvider.provideMetrics(Accessors.getNode(member), metricsCollectionContext);
        InOrder inOrder = verifyMetrics(2, 3, true);
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_HEAP_USAGE, calculateMinimumVectorServiceHeapUsage());
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_VECTOR_API_AVAILABLE, VectorPlatformUtil.isVectorModulePresent());
    }

    @Test
    void testPhoneHome_whenVectorCollectionUsed() {
        givenVectorCollectionsConfigured();
        VectorCollection<Integer, Integer> vc = member.getVectorCollection("test2");
        vc.putAsync(1, VectorDocument.of(1, randomVec(1536))).toCompletableFuture().join();
        vectorCollectionMetricsProvider.provideMetrics(Accessors.getNode(member), metricsCollectionContext);

        verifyMetrics(2, 3, calculateMinimumVectorServiceHeapUsage());
    }

    private void givenVectorCollectionsConfigured() {
        VectorIndexConfig vic1 = new VectorIndexConfig("index1", Metric.EUCLIDEAN, 2);
        VectorIndexConfig vic2 = new VectorIndexConfig("index2", Metric.EUCLIDEAN, 128);
        VectorCollectionConfig test1Config = new VectorCollectionConfig("test1")
                .addVectorIndexConfig(vic1).addVectorIndexConfig(vic2)
                .setBackupCount(1)
                .setAsyncBackupCount(2);

        VectorIndexConfig vic3 = new VectorIndexConfig("index", Metric.EUCLIDEAN, 1536);
        VectorCollectionConfig test2Config = new VectorCollectionConfig("test2")
                .addVectorIndexConfig(vic3)
                .setBackupCount(2)
                .setAsyncBackupCount(1);

        Config config = HazelcastTestSupport.smallInstanceConfig()
                .addVectorCollectionConfig(test1Config)
                .addVectorCollectionConfig(test2Config);

        member = hazelcastFactory.newHazelcastInstance(config);
    }

    private void verifyMetrics(int collectionCount, int indexCount, long heapGreaterThan) {
        InOrder inOrder = verifyMetrics(collectionCount, indexCount, true);
        inOrder.verify(metricsCollectionContext)
                .collect(eq(VECTOR_COLLECTION_HEAP_USAGE), gt(heapGreaterThan));
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_VECTOR_API_AVAILABLE, VectorPlatformUtil.isVectorModulePresent());
        inOrder.verifyNoMoreInteractions();
    }

    private InOrder verifyMetrics(int collectionCount, int indexCount, boolean indexConfigsExist) {
        InOrder inOrder = inOrder(metricsCollectionContext);
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_COUNT, collectionCount);
        inOrder.verify(metricsCollectionContext)
                .collect(VECTOR_COLLECTION_INDEX_COUNT, indexCount);
        if (indexConfigsExist) {
            inOrder.verify(metricsCollectionContext)
                    .collect(eq(VECTOR_COLLECTION_INDEX_DIMENSIONS), ArgumentMatchers.assertArg(o -> {
                        if (o instanceof String dimensions) {
                            assertTrue(dimensions.contains("1536"),
                                    "Dimensions string was expected to contain 1536, actual value was " + dimensions);
                            assertTrue(dimensions.contains("2"),
                                    "Dimensions string was expected to contain 2, actual value was " + dimensions);
                            assertTrue(dimensions.contains("128"),
                                    "Dimensions string was expected to contain 128, actual value was " + dimensions);
                        }
                    }));
            inOrder.verify(metricsCollectionContext)
                    .collect(eq(VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS), ArgumentMatchers.assertArg(o -> {
                        if (o instanceof String backupCounts) {
                            assertTrue(backupCounts.contains("3"),
                                    "Backup counts string was expected to contain 3 (sync+async), actual value was " + backupCounts);
                        }
                    }));
        } else {
            inOrder.verify(metricsCollectionContext).collect(VECTOR_COLLECTION_INDEX_DIMENSIONS, "");
            inOrder.verify(metricsCollectionContext).collect(VECTOR_COLLECTION_TOTAL_BACKUP_COUNTS, "");
        }
        return inOrder;
    }
}
