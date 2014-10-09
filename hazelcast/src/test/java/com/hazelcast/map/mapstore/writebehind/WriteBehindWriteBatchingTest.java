package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindWriteBatchingTest extends HazelcastTestSupport {

    @Test
    public void testWriteBatching() throws Exception {
        final int writeBatchSize = 8;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .withWriteBatchSize(writeBatchSize)
                .build();

        final int numberOfItems = 1024;
        populateMap(map, numberOfItems);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // expecting more than half of operations should have the write batch size.
                // takes this a lower bound.
                final int expectedBatchOpCount = (numberOfItems / writeBatchSize) / 2;
                final int numberOfBatchOperationsEqualWriteBatchSize = mapStore.findNumberOfBatchsEqualWriteBatchSize(writeBatchSize);
                assertTrue(numberOfBatchOperationsEqualWriteBatchSize >= expectedBatchOpCount);
            }
        }, 20);

    }

    private void populateMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

}
