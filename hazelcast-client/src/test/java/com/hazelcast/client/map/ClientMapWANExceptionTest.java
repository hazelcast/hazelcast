package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WANReplicationQueueFullException;
import com.hazelcast.wan.impl.FullQueueWanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapWANExceptionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance(getConfig());
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testMapPut() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        map.put(1, 1);
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testMapPutAll() {
        IMap<Object, Object> map = client.getMap("wan-exception-client-test-map");
        Map<Object, Object> inputMap = MapUtil.createHashMap(1);
        inputMap.put(1, 1);
        map.putAll(inputMap);
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName("dummyWan");

        wanConfig.addWanPublisherConfig(getWanPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName("dummyWan");
        wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());

        config.addWanReplicationConfig(wanConfig);
        config.getMapConfig("default").setWanReplicationRef(wanRef);
        return config;
    }

    private WanPublisherConfig getWanPublisherConfig() {
        WanPublisherConfig target = new WanPublisherConfig();
        target.setClassName(FullQueueWanReplication.class.getName());
        return target;
    }
}

