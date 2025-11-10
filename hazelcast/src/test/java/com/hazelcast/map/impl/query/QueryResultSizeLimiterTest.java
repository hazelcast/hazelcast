/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK;
import static com.hazelcast.spi.properties.ClusterProperty.QUERY_RESULT_SIZE_LIMIT;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryResultSizeLimiterTest {

    private static final String ANY_MAP_NAME = "foobar";
    private static final int PARTITION_COUNT = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());

    private final Map<Integer, Integer> localPartitions = new HashMap<>();

    private final LocalMapStatsImpl localMapStats = mock(LocalMapStatsImpl.class);

    private QueryResultSizeLimiter limiter;

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultResultSizeLimitNegative() {
        initMocksWithConfiguration(-2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultResultSizeLimitZero() {
        initMocksWithConfiguration(0);
    }

    @Test
    public void testNodeResultFeatureDisabled() {
        initMocksWithConfiguration(-1);
        assertFalse(limiter.isQueryResultLimitEnabled());
    }

    @Test
    public void testNodeResultFeatureEnabled() {
        initMocksWithConfiguration(1);
        assertTrue(limiter.isQueryResultLimitEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultPreCheckLimitNegative() {
        initMocksWithConfiguration(Integer.MAX_VALUE, -2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultPreCheckLimitZero() {
        initMocksWithConfiguration(Integer.MAX_VALUE, 0);
    }

    @Test
    public void testNodeResultPreCheckLimitDisabled() {
        initMocksWithConfiguration(Integer.MAX_VALUE, -1);
        assertTrue(limiter.isQueryResultLimitEnabled());
        assertFalse(limiter.isPreCheckEnabled());
    }

    @Test
    public void testNodeResultPreCheckLimitEnabled() {
        initMocksWithConfiguration(Integer.MAX_VALUE, 1);
        assertTrue(limiter.isQueryResultLimitEnabled());
        assertTrue(limiter.isPreCheckEnabled());
    }

    @Test
    public void testNodeResultLimitMinResultLimit() {
        initMocksWithConfiguration(QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT, 3);
        long nodeResultLimit1 = limiter.getNodeResultLimit(1);

        initMocksWithConfiguration(QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT / 2, 3);
        long nodeResultLimit2 = limiter.getNodeResultLimit(1);

        assertEquals(nodeResultLimit1, nodeResultLimit2);
    }

    @Test
    public void testNodeResultLimitSinglePartition() {
        initMocksWithConfiguration(200000, 3);

        assertEquals(849, limiter.getNodeResultLimit(1));
    }

    @Test
    public void testNodeResultLimitThreePartitions() {
        initMocksWithConfiguration(200000, 3);

        assertEquals(2547, limiter.getNodeResultLimit(3));
    }

    @Test
    public void testLocalPreCheckDisabled() {
        initMocksWithConfiguration(200000, QueryResultSizeLimiter.DISABLED);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWithNoLocalPartitions() {
        initMocksWithConfiguration(200000, 1);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWithEmptyPartition() {
        int[] partitionsSizes = {0};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitPartitionBelowLimit() {
        int[] partitionsSizes = {848};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitPartitionOverLimit() {
        int[] partitionsSizes = {1090};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitTwoPartitionsBelowLimit() {
        int[] partitionsSizes = {849, 849};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitTwoPartitionsOverLimit() {
        int[] partitionsSizes = {1062, 1063};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 2);

        assertThrows(QueryResultSizeExceededException.class,
                () -> limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME));

        verify(localMapStats).incrementQueryResultSizeExceededCount();

    }

    @Test
    public void testLocalPreCheckEnabledWitMorePartitionsThanPreCheckThresholdBelowLimit() {
        int[] partitionSizes = {849, 849, Integer.MAX_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitMorePartitionsThanPreCheckThresholdOverLimit() {
        int[] partitionSizes = {1200, 1000, Integer.MIN_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitDifferentPartitionSizesBelowLimit() {
        int[] partitionSizes = {566, 1132, Integer.MAX_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitDifferentPartitionSizesOverLimit() {
        int[] partitionSizes = {0, 2200, Integer.MIN_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.precheckMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    private void initMocksWithConfiguration(int maxResultSizeLimit) {
        initMocksWithConfiguration(maxResultSizeLimit, Integer.MAX_VALUE);
    }

    private void initMocksWithConfiguration(int maxResultSizeLimit, int maxLocalPartitionLimitForPreCheck) {
        Config config = new Config();
        config.setProperty(QUERY_RESULT_SIZE_LIMIT.getName(), valueOf(maxResultSizeLimit));
        config.setProperty(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getName(), valueOf(maxLocalPartitionLimitForPreCheck));
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), valueOf(PARTITION_COUNT));

        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);

        InternalPartitionService partitionService = mock(InternalPartitionService.class);
        when(partitionService.getPartitionCount()).thenReturn(PARTITION_COUNT);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getProperties()).thenReturn(hazelcastProperties);
        when(nodeEngine.getPartitionService()).thenReturn(partitionService);

        RecordStore recordStore = mock(RecordStore.class);
        when(recordStore.size()).then(new RecordStoreAnswer(localPartitions.values()));

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(mapServiceContext.getRecordStore(anyInt(), anyString())).thenReturn(recordStore);
        when(mapServiceContext.getCachedOwnedPartitions()).thenReturn(new PartitionIdSet(PARTITION_COUNT, localPartitions.keySet()));

        LocalMapStatsProvider localMapStatsProvider = mock(LocalMapStatsProvider.class);
        when(mapServiceContext.getLocalMapStatsProvider()).thenReturn(localMapStatsProvider);
        when(localMapStatsProvider.hasLocalMapStatsImpl(anyString())).thenReturn(true);
        when(localMapStatsProvider.getLocalMapStatsImpl(anyString())).thenReturn(localMapStats);

        limiter = new QueryResultSizeLimiter(mapServiceContext, Logger.getLogger(QueryResultSizeLimiterTest.class));
    }

    private void populatePartitions(int[] localPartitionSize) {
        for (int i = 0; i < localPartitionSize.length; i++) {
            localPartitions.put(i, localPartitionSize[i]);
        }
    }

    private static final class RecordStoreAnswer implements Answer<Integer> {

        private final Iterator<Integer> iterator;

        private Integer lastValue = null;

        private RecordStoreAnswer(Collection<Integer> partitionSizes) {
            this.iterator = partitionSizes.iterator();
        }

        @Override
        public Integer answer(InvocationOnMock invocation) {
            if (iterator.hasNext()) {
                lastValue = iterator.next();
            }
            return lastValue;
        }

    }

}
