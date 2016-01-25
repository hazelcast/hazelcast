package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests for WAN replication API
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(3);
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
    }

    @Test
    public void mapPutRemoveTest() {
        IMap<Object, Object> map = instance1.getMap("dummy-wan-test-map");

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
            map.remove(i);
        }

        DummyWanReplication impl1 = getWanReplicationImpl(instance1);
        DummyWanReplication impl2 = getWanReplicationImpl(instance2);

        //Number of total events should be 20. (10 put, 10 remove ops)
        assertEquals(20, impl1.eventQueue.size() + impl2.eventQueue.size());
    }

    @Test
    public void entryProcessorTest() throws Exception {
        IMap<Object, Object> map = instance1.getMap("dummy-wan-entryprocessor-test-map");

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        DummyWanReplication impl1 = getWanReplicationImpl(instance1);
        DummyWanReplication impl2 = getWanReplicationImpl(instance2);
        assertEquals(10, impl1.eventQueue.size() + impl2.eventQueue.size());

        //Clean event queues
        impl1.eventQueue.clear();
        impl2.eventQueue.clear();

        Set keySet = new HashSet();
        for (int i = 0; i < 10; i++) {
            keySet.add(getSerializationService(instance1).toData(i));
        }

        //Multiple entry operations (update)
        OperationFactory operationFactory
                = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(), keySet, new UpdatingEntryProcessor());

        InternalOperationService operationService = getOperationService(instance1);
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        //There should be 10 events since all entries should be processed
        assertEquals(10, impl1.eventQueue.size() + impl2.eventQueue.size());

        //Multiple entry operations (remove)
        OperationFactory deletingOperationFactory
                = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(), keySet, new DeletingEntryProcessor());
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, deletingOperationFactory);

        //10 more event should be published
        assertEquals(20, impl1.eventQueue.size() + impl2.eventQueue.size());
    }

    @Test
    public void programmaticImplCreationTest() {
        Config config = getConfig();
        WanTargetClusterConfig targetClusterConfig = config.getWanReplicationConfig("dummyWan").getTargetClusterConfigs().get(0);
        DummyWanReplication dummyWanReplication = new DummyWanReplication();
        targetClusterConfig.setReplicationImplObject(dummyWanReplication);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        assertEquals(dummyWanReplication, getWanReplicationImpl(instance));
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("dummyWan");

        wanConfig.addTargetClusterConfig(getTargetClusterConfig());

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("dummyWan");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());

        config.addWanReplicationConfig(wanConfig);
        config.getMapConfig("default").setWanReplicationRef(wanRef);
        return config;
    }

    private WanTargetClusterConfig getTargetClusterConfig() {
        WanTargetClusterConfig target = new WanTargetClusterConfig();
        target.setReplicationImpl(DummyWanReplication.class.getName());
        target.addEndpoint("127.0.0.1:9999");
        return target;
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

    private static class UpdatingEntryProcessor
            implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

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

    private static class DeletingEntryProcessor
            implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

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

