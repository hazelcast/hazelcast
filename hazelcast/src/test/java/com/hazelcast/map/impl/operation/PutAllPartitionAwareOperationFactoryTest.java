package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PutAllPartitionAwareOperationFactoryTest extends HazelcastTestSupport {

    private PutAllPartitionAwareOperationFactory factory;

    @Before
    public void setUp() throws Exception {
        String name = randomMapName();
        int[] partitions = new int[0];
        MapEntries[] mapEntries = new MapEntries[0];
        factory = getFactory(name, partitions, mapEntries);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateOperation() {
        factory.createOperation();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatePartitionOperation() {
        factory.createPartitionOperation(0);
    }

    protected PutAllPartitionAwareOperationFactory getFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        return new PutAllPartitionAwareOperationFactory(name, partitions, mapEntries);
    }
}
