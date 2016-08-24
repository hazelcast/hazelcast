package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.nearcache.NearCacheLiteMemberTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapNearCacheLiteMemberTest {

    private String mapName;

    private TestHazelcastFactory factory;

    private HazelcastInstance lite;
    private HazelcastInstance client;

    @Before
    public void init() {
        mapName = randomMapName();
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(NearCacheLiteMemberTest.createConfig(mapName, false));
        lite = factory.newHazelcastInstance(NearCacheLiteMemberTest.createConfig(mapName, true));
        client = factory.newHazelcastClient();
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testPut() {
        NearCacheLiteMemberTest.testPut(client, lite, mapName);
    }

    @Test
    public void testPutAll() {
        NearCacheLiteMemberTest.testPutAll(client, lite, mapName);
    }

    @Test
    public void testPutTransient() {
        NearCacheLiteMemberTest.testPutTransient(client, lite, mapName);
    }

    @Test
    public void testSet() {
        NearCacheLiteMemberTest.testSet(client, lite, mapName);
    }

    @Test
    public void testUpdate() {
        NearCacheLiteMemberTest.testUpdate(client, lite, mapName);
    }

    @Test
    public void testUpdateWithSet() {
        NearCacheLiteMemberTest.testUpdateWithSet(client, lite, mapName);
    }

    @Test
    public void testUpdateWithPutAll() {
        NearCacheLiteMemberTest.testUpdateWithPutAll(client, lite, mapName);
    }

    @Test
    public void testReplace() {
        NearCacheLiteMemberTest.testReplace(client, lite, mapName);
    }

    @Test
    public void testEvict() {
        NearCacheLiteMemberTest.testEvict(client, lite, mapName);
    }

    @Test
    public void testRemove() {
        NearCacheLiteMemberTest.testRemove(client, lite, mapName);
    }

    @Test
    public void testDelete() {
        NearCacheLiteMemberTest.testDelete(client, lite, mapName);
    }

    @Test
    public void testClear() {
        NearCacheLiteMemberTest.testClear(client, lite, mapName);
    }

    @Test
    public void testEvictAll() {
        NearCacheLiteMemberTest.testEvictAll(client, lite, mapName);
    }

    @Test
    public void testExecuteOnKey() {
        NearCacheLiteMemberTest.testExecuteOnKey(client, lite, mapName);
    }

    @Test
    public void testExecuteOnKeys() {
        NearCacheLiteMemberTest.testExecuteOnKeys(client, lite, mapName);
    }
}
