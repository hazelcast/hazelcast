package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractBaseReplicatedRecordStoreTest extends HazelcastTestSupport {

    TestReplicatedRecordStore recordStore;
    TestReplicatedRecordStore recordStoreSameAttributes;

    TestReplicatedRecordStore recordStoreOtherStorage;
    TestReplicatedRecordStore recordStoreOtherName;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        recordStore = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreSameAttributes = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreSameAttributes.storageRef.set(recordStore.storageRef.get());

        recordStoreOtherStorage = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreOtherName = new TestReplicatedRecordStore("otherRecordStore", service, 0);
    }

    @After
    public void tearDown() throws Exception {
        shutdownNodeFactory();
    }

    @Test
    public void testGetRecords() {
        assertTrue(recordStore.getRecords().isEmpty());

        recordStore.put("key1", "value1");
        recordStore.put("key2", "value2");

        assertEquals(2, recordStore.getRecords().size());
    }

    @Test
    public void testEquals() {
        assertEquals(recordStore, recordStore);
        assertEquals(recordStoreSameAttributes, recordStore);

        assertNotEquals(recordStore, null);
        assertNotEquals(recordStore, new Object());

        assertNotEquals(recordStoreOtherStorage, recordStore);
        assertNotEquals(recordStoreOtherName, recordStore);
    }

    @Test
    public void testHashCode() {
        assertEquals(recordStore.hashCode(), recordStore.hashCode());
        assertEquals(recordStoreSameAttributes.hashCode(), recordStore.hashCode());

        assertNotEquals(recordStoreOtherStorage.hashCode(), recordStore.hashCode());
        assertNotEquals(recordStoreOtherName.hashCode(), recordStore.hashCode());
    }

    private class TestReplicatedRecordStore extends AbstractReplicatedRecordStore<String, String> {

        public TestReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
            super(name, replicatedMapService, partitionId);
        }

        @Override
        public Object unmarshall(Object key) {
            return key;
        }

        @Override
        public Object marshall(Object key) {
            return key;
        }
    }
}
