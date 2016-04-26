package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK;
import static com.hazelcast.spi.properties.GroupProperty.QUERY_RESULT_SIZE_LIMIT;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryResultSizeLimiterTest {

    private static final String ANY_MAP_NAME = "foobar";
    private static final int DEFAULT_PARTITION_COUNT = 271;

    private final Map<Integer, Integer> localPartitions = new HashMap<Integer, Integer>();

    private QueryResultSizeLimiter limiter;

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultResultSizeLimitNegative() {
        initMocksWithConfiguration(-2, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultResultSizeLimitZero() {
        initMocksWithConfiguration(0, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void testNodeResultFeatureDisabled() {
        initMocksWithConfiguration(-1, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertFalse(limiter.isQueryResultLimitEnabled());
    }

    @Test
    public void testNodeResultFeatureEnabled() {
        initMocksWithConfiguration(1, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertTrue(limiter.isQueryResultLimitEnabled());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultPreCheckLimitNegative() {
        initMocksWithConfiguration(Integer.MAX_VALUE, -2, Integer.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNodeResultPreCheckLimitZero() {
        initMocksWithConfiguration(Integer.MAX_VALUE, 0, Integer.MAX_VALUE);
    }

    @Test
    public void testNodeResultPreCheckLimitDisabled() {
        initMocksWithConfiguration(Integer.MAX_VALUE, -1, Integer.MAX_VALUE);
        assertTrue(limiter.isQueryResultLimitEnabled());
        assertFalse(limiter.isPreCheckEnabled());
    }

    @Test
    public void testNodeResultPreCheckLimitEnabled() {
        initMocksWithConfiguration(Integer.MAX_VALUE, 1, Integer.MAX_VALUE);
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

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWithNoLocalPartitions() {
        initMocksWithConfiguration(200000, 1);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWithEmptyPartition() {
        int[] partitionsSizes = {0};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitPartitionBelowLimit() {
        int[] partitionsSizes = {848};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitPartitionOverLimit() {
        int[] partitionsSizes = {850};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 1);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitTwoPartitionsBelowLimit() {
        int[] partitionsSizes = {849, 849};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitTwoPartitionsOverLimit() {
        int[] partitionsSizes = {849, 850};
        populatePartitions(partitionsSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitMorePartitionsThanPreCheckThresholdBelowLimit() {
        int[] partitionSizes = {849, 849, Integer.MAX_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitMorePartitionsThanPreCheckThresholdOverLimit() {
        int[] partitionSizes = {849, 850, Integer.MIN_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test
    public void testLocalPreCheckEnabledWitDifferentPartitionSizesBelowLimit() {
        int[] partitionSizes = {566, 1132, Integer.MAX_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testLocalPreCheckEnabledWitDifferentPartitionSizesOverLimit() {
        int[] partitionSizes = {0, 1699, Integer.MIN_VALUE};
        populatePartitions(partitionSizes);
        initMocksWithConfiguration(200000, 2);

        limiter.checkMaxResultLimitOnLocalPartitions(ANY_MAP_NAME);
    }

    private void initMocksWithConfiguration(int maxResultSizeLimit, int maxLocalPartitionLimitForPreCheck) {
        initMocksWithConfiguration(maxResultSizeLimit, maxLocalPartitionLimitForPreCheck, DEFAULT_PARTITION_COUNT);
    }

    private void initMocksWithConfiguration(int maxResultSizeLimit, int maxLocalPartitionLimitForPreCheck, int partitionCount) {
        Config config = new Config();
        config.setProperty(QUERY_RESULT_SIZE_LIMIT.getName(), valueOf(maxResultSizeLimit));
        config.setProperty(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getName(), valueOf(maxLocalPartitionLimitForPreCheck));
        config.setProperty(PARTITION_COUNT.getName(), valueOf(partitionCount));

        HazelcastProperties hazelcastProperties = new HazelcastProperties(config);

        InternalPartitionService partitionService = mock(InternalPartitionService.class);
        when(partitionService.getPartitionCount()).thenReturn(partitionCount);

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getProperties()).thenReturn(hazelcastProperties);
        when(nodeEngine.getPartitionService()).thenReturn(partitionService);

        RecordStore recordStore = mock(RecordStore.class);
        when(recordStore.size()).then(new RecordStoreAnswer(localPartitions.values()));

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(mapServiceContext.getRecordStore(anyInt(), anyString())).thenReturn(recordStore);
        when(mapServiceContext.getOwnedPartitions()).thenReturn(localPartitions.keySet());

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
        public Integer answer(InvocationOnMock invocation) throws Throwable {
            if (iterator.hasNext()) {
                lastValue = iterator.next();
            }
            return lastValue;
        }
    }
}
