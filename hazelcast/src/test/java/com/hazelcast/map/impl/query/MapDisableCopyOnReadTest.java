package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;
import com.hazelcast.map.Immutable;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

public class MapDisableCopyOnReadTest extends HazelcastTestSupport {

    private MapProxyImpl<String, TestEntityImmutable> immutableMapProxy;
    private MapProxyImpl<String, TestEntity> mutableMapProxy;
    private PartitionService partitionService;
    private HazelcastInstance instance;

    @Before
    public void setup() {
        Config config = new Config().addMapConfig(new MapConfig("myMap*").setInMemoryFormat(InMemoryFormat.OBJECT));
        instance = createHazelcastInstance(config);
        partitionService = instance.getPartitionService();
        immutableMapProxy = (MapProxyImpl)instance.getMap("myMapImmutable");
        mutableMapProxy = (MapProxyImpl)instance.getMap("myMapMutable");
    }

    @Test
    public void testGetImmutable() {
        TestEntityImmutable tc = new TestEntityImmutable("hello");
        immutableMapProxy.put("testCor", tc);
        TestEntityImmutable result1 = immutableMapProxy.get("testCor");
        TestEntityImmutable result2 = immutableMapProxy.get("testCor");
        Assert.assertTrue(result1 == result2);
    }

    @Test
    public void testGetMutable() {
        TestEntity tc = new TestEntity("hello");
        mutableMapProxy.put("testCor", tc);
        TestEntity result1 = mutableMapProxy.get("testCor");
        TestEntity result2 = mutableMapProxy.get("testCor");
        Assert.assertFalse(result1 == result2);
    }

    @Test
    public void testMapIteratorImmutable() {
        String key = "testCorKey";
        TestEntityImmutable tc = new TestEntityImmutable("hello");
        immutableMapProxy.put(key, tc);
        int partitionId = partitionService.getPartition(key).getPartitionId();

        TestEntityImmutable result1 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        TestEntityImmutable result2 =  immutableMapProxy.iterator(10, partitionId, false).next().getValue();
        Assert.assertTrue(result1 == result2);
    }

    @Test
    public void testMapIteratorForMutable() {
        String key = "testCorKey";
        TestEntity tc = new TestEntity("hello");
        mutableMapProxy.put(key, tc);
        int partitionId = partitionService.getPartition(key).getPartitionId();

        TestEntity result1 =  mutableMapProxy.iterator(10, partitionId, false).next().getValue();
        TestEntity result2 =  mutableMapProxy.iterator(10, partitionId, false).next().getValue();
        Assert.assertFalse(result1 == result2);
    }

}

class TestEntityImmutable implements Immutable, Serializable {
    private static final long serialVersionUID = 1L;
    private String test;

    public TestEntityImmutable(String test) {
        this.test = test;
    }

    public String getTest() {
        return test;
    }
}

class TestEntity implements Serializable {
    private static final long serialVersionUID = 1L;
    private String test;

    public TestEntity(String test) {
        this.test = test;
    }

    public String getTest() {
        return test;
    }
}