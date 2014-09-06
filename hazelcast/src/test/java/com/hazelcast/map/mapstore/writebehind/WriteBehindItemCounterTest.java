package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindItemCounterTest extends HazelcastTestSupport {

    @Test
    public void testCounter_against_one_node_zero_backup() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(100);
        final IMap<Object, Object> map = builder.build();
        final int maxCapacity = getMaxCapacity(builder.getNodes()[0]);
        populateMap(map, maxCapacity);

        assertEquals(maxCapacity, map.size());
    }


    @Test
    public void testCounter_against_many_nodes() throws Exception {
        final int nodeCount = 2;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withBackupCount(0)
                .withWriteDelaySeconds(100);
        final IMap<Object, Object> map = builder.build();
        final int maxCapacity = getMaxCapacity(builder.getNodes()[0]);
        populateMap(map, maxCapacity + 3);

        assertTrue(map.size() > maxCapacity);
    }

    private void populateMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

    private int getMaxCapacity(HazelcastInstance node) {
        return getNode(node).getNodeEngine().getGroupProperties().MAP_WRITE_BEHIND_QUEUE_CAPACITY.getInteger();
    }
}
