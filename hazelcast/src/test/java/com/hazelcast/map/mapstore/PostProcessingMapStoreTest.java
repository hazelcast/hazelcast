package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.PostProcessingMapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PostProcessingMapStoreTest extends HazelcastTestSupport {

    @Test
    public void testProcessedValueCarriedToTheBackup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        String name = randomString();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setReadBackupData(true);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setClassName(IncrementerPostProcessingMapStore.class.getName());
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Integer, SampleObject> map1 = instance1.getMap(name);
        IMap<Integer, SampleObject> map2 = instance2.getMap(name);

        for (int i = 0; i < 100; i++) {
            map1.put(i, new SampleObject(i));
        }

        for (int i = 0; i < 100; i++) {
            SampleObject o = map1.get(i);
            assertEquals(i + 1, o.version);
        }

        for (int i = 0; i < 100; i++) {
            SampleObject o = map2.get(i);
            assertEquals(i + 1, o.version);
        }

    }

    public static class IncrementerPostProcessingMapStore implements MapStore<Integer, SampleObject>, PostProcessingMapStore {

        Map<Integer, SampleObject> map = new HashMap<Integer, SampleObject>();

        @Override
        public void store(Integer key, SampleObject value) {
            value.version++;
            map.put(key, value);
        }

        @Override
        public void storeAll(Map<Integer, SampleObject> map) {
            for (Map.Entry<Integer, SampleObject> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(Integer key) {
            map.remove(key);
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
            for (Integer key : keys) {
                map.remove(key);
            }
        }

        @Override
        public SampleObject load(Integer key) {
            return map.get(key);
        }

        @Override
        public Map<Integer, SampleObject> loadAll(Collection<Integer> keys) {
            HashMap<Integer, SampleObject> temp = new HashMap<Integer, SampleObject>();
            for (Integer key : keys) {
                temp.put(key, map.get(key));
            }
            return temp;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            return map.keySet();
        }
    }

    public static class SampleObject implements Serializable {

        public int version;

        public SampleObject(int version) {
            this.version = version;
        }
    }
}
