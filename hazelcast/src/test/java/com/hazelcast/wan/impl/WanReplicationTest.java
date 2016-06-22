package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for WAN replication API.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private IMap<Object, Object> map;

    private DummyWanReplication impl1;
    private DummyWanReplication impl2;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void mapPutReplaceRemoveTest() {
        initInstancesAndMap("wan-replication-test-put-replace-remove");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
            map.replace(i, i * 2);
            map.remove(i);
        }

        assertTotalQueueSize(30);
    }

    @Test
    public void mapSetReplaceRemoveIfSameTest() {
        initInstancesAndMap("wan-replication-test-set-replace-remove-if-same");
        for (int i = 0; i < 10; i++) {
            map.set(i, i);
            map.replace(i, i, i * 2);
            map.remove(i, i * 2);
        }

        assertTotalQueueSize(30);
    }

    @Test
    public void mapTryPutRemoveTest() {
        initInstancesAndMap("wan-replication-test-try-put-remove");
        for (int i = 0; i < 10; i++) {
            assertTrue(map.tryPut(i, i, 10, TimeUnit.SECONDS));
            assertTrue(map.tryRemove(i, 10, TimeUnit.SECONDS));
        }

        assertTotalQueueSize(20);
    }

    @Test
    public void mapPutIfAbsentDeleteTest() {
        initInstancesAndMap("wan-replication-test-put-if-absent-delete");
        for (int i = 0; i < 10; i++) {
            assertNull(map.putIfAbsent(i, i));
            map.delete(i);
        }

        assertTotalQueueSize(20);
    }

    @Test
    public void mapPutTransientTest() {
        initInstancesAndMap("wan-replication-test-put-transient");
        for (int i = 0; i < 10; i++) {
            map.putTransient(i, i, 1, TimeUnit.SECONDS);
        }

        assertTotalQueueSize(10);
    }

    @Test
    public void mapPutAllTest() {
        Map<Object, Object> userMap = new HashMap<Object, Object>();
        for (int i = 0; i < 10; i++) {
            userMap.put(i, i);
        }

        initInstancesAndMap("wan-replication-test-put-all");
        map.putAll(userMap);

        assertTotalQueueSize(10);
    }

    @Test
    public void entryProcessorTest() throws Exception {
        initInstancesAndMap("wan-replication-test-entry-processor");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        assertTotalQueueSize(10);

        // clean event queues
        impl1.eventQueue.clear();
        impl2.eventQueue.clear();

        InternalSerializationService serializationService = getSerializationService(instance1);
        Set<Data> keySet = new HashSet<Data>();
        for (int i = 0; i < 10; i++) {
            keySet.add(serializationService.toData(i));
        }

        // multiple entry operations (update)
        OperationFactory operationFactory = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(), keySet,
                new UpdatingEntryProcessor());

        InternalOperationService operationService = getOperationService(instance1);
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        // there should be 10 events since all entries should be processed
        assertTotalQueueSize(10);

        // multiple entry operations (remove)
        OperationFactory deletingOperationFactory = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(),
                keySet, new DeletingEntryProcessor());
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, deletingOperationFactory);

        // 10 more event should be published
        assertTotalQueueSize(20);
    }

    @Test
    public void programmaticImplCreationTest() {
        Config config = getConfig();
        WanPublisherConfig publisherConfig = config.getWanReplicationConfig("dummyWan").getWanPublisherConfigs().get(0);
        DummyWanReplication dummyWanReplication = new DummyWanReplication();
        publisherConfig.setImplementation(dummyWanReplication);
        instance1 = factory.newHazelcastInstance(config);

        assertEquals(dummyWanReplication, getWanReplicationImpl(instance1));
    }

    private void initInstancesAndMap(String name) {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        map = instance1.getMap(name);
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("dummyWan");

        wanConfig.addWanPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("dummyWan");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());

        config.addWanReplicationConfig(wanConfig);
        config.getMapConfig("default").setWanReplicationRef(wanRef);
        return config;
    }

    private WanPublisherConfig getPublisherConfig() {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setClassName(DummyWanReplication.class.getName());
        return publisherConfig;
    }

    private DummyWanReplication getWanReplicationImpl(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        WanReplicationPublisherDelegate delegate = (WanReplicationPublisherDelegate) service.getWanReplicationPublisher("dummyWan");
        return (DummyWanReplication) delegate.getEndpoints()[0];
    }

    private MapOperationProvider getOperationProvider(Map map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapProxy.getName());
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        if (impl1 == null) {
            impl1 = getWanReplicationImpl(instance1);
            impl2 = getWanReplicationImpl(instance2);
        }

        final Queue<WanReplicationEvent> eventQueue1 = impl1.eventQueue;
        final Queue<WanReplicationEvent> eventQueue2 = impl2.eventQueue;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size());
            }
        });
    }

    private static class UpdatingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue("EP" + entry.getValue());
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }

    private static class DeletingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue(null);
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }
}
