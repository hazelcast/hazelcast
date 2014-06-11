package com.hazelcast.map.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindWriteCoalescingTest extends HazelcastTestSupport {

    @Test
    public void testWriteCoalescing_whenEnabled_manyUpdatesOnSameKey() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateSameKey(map, numberOfUpdates, key);

        sleepSeconds(5);

        final int expectedTotalStoreCount = 1;
        final int expectedValue = numberOfUpdates - 1;

        assertEquals(expectedTotalStoreCount, mapStore.getStoreOpCount());
        assertEquals(expectedValue, map.get(key));
    }

    private void continuouslyUpdateSameKey(IMap map, int numberOfUpdates, int key) {
        for (int i = 0; i < numberOfUpdates; i++) {
            sleepMillis(1);
            map.put(key, i);
        }
    }
}
