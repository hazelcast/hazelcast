package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;

public abstract class AbstractQueryCacheTestSupport extends HazelcastTestSupport {

    protected Config config = new Config();
    protected String mapName = randomString();

    protected HazelcastInstance[] instances;
    protected IMap<Integer, Employee> map;

    @Before
    public void setUp() {
        prepare();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        instances = factory.newInstances(config);
        map = getMap(instances[0], mapName);
    }

    /**
     * Override this method to adjust the test configuration.
     */
    void prepare() {
    }

    void populateMap(IMap<Integer, Employee> map, int count) {
        populateMap(map, 0, count);
    }

    void populateMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.put(i, new Employee(i));
        }
    }

    void removeEntriesFromMap(IMap<Integer, Employee> map, int startIndex, int endIndex) {
        for (int i = startIndex; i < endIndex; i++) {
            map.remove(i);
        }
    }

    public static <K, V> IMap<K, V> getMap(HazelcastInstance instance, String mapName) {
        return instance.getMap(mapName);
    }
}
