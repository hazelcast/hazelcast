package com.hazelcast.map.impl.record;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SampleObjects;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


/**
 * This class uses basic map functionality with different configuration than existing ones.
 * Actually, it tests {@link ObjectRecord}'s getValue and setValue.
 */

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ObjectRecordTest extends HazelcastTestSupport {

    IMap<Integer, Object> map;

    @Before
    public void setup() {
        String mapName = randomMapName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setStatisticsEnabled(false);
        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap(mapName);
    }

    @Test
    public void testGetValue() throws Exception {
        map.put(1, new SampleObjects.Employee("alex", 26, true, 25));
        map.get(1);
        assertSizeEventually(1, map);
    }

    @Test
    public void testSetValue() throws Exception {
        map.put(1, new SampleObjects.Employee("alex", 26, true, 25));
        map.put(1, new SampleObjects.Employee("tom", 24, true, 10));
        assertSizeEventually(1, map);
    }
}