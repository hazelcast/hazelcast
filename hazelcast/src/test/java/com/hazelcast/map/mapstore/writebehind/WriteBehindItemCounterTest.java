package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.mapstore.writebehind.ReachedMaxSizeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test is targeted to be used when {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} is set false.
 * When it is false, this means we are trying to persist all updates on an entry in contrast with write-coalescing.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindItemCounterTest extends HazelcastTestSupport {

    @Test
    public void testCounter_against_one_node_zero_backup() throws Exception {
        final int maxCapacityPerNode = 100;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode);

        final IMap<Object, Object> map = builder.build();
        populateMap(map, maxCapacityPerNode);

        assertEquals(maxCapacityPerNode, map.size());
    }


    @Test
    public void testCounter_against_many_nodes() throws Exception {
        final int maxCapacityPerNode = 100;
        final int nodeCount = 2;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(nodeCount))
                .withBackupCount(0)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode)
                .withWriteDelaySeconds(100);
        final IMap<Object, Object> map = builder.build();
        // put slightly more number of entries which is higher than max write-behind queue capacity per node.
        populateMap(map, maxCapacityPerNode + 3);

        assertTrue(map.size() > maxCapacityPerNode);
    }


    @Test(expected = ReachedMaxSizeException.class)
    public void testCounter_whenMaxCapacityExceeded() throws Exception {
        final int maxCapacityPerNode = 100;
        final int nodeCount = 1;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode)
                .withWriteDelaySeconds(100);
        final IMap<Object, Object> map = builder.build();

        // exceed max write-behind queue capacity per node.
        populateMap(map, 2 * maxCapacityPerNode);
    }

    private void populateMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }


}