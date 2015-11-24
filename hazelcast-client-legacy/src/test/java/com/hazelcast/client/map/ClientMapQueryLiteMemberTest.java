package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.MapLiteMemberTest;
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
public class ClientMapQueryLiteMemberTest {

    private TestHazelcastFactory factory;

    private IMap<Integer, Integer> map;

    @Before
    public void setUp()
            throws Exception {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance(new Config().setLiteMember(true));
        HazelcastInstance client = factory.newHazelcastClient();
        map = client.getMap(randomMapName());
    }

    @After
    public void tearDown()
            throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testMapValuesQuery() {
        MapLiteMemberTest.testMapValuesQuery(map);
    }

    @Test
    public void testMapKeysQuery() {
        MapLiteMemberTest.testMapKeysQuery(map);
    }

}
