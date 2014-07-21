package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.mapstore.MapStoreTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLoaderExceptionHandling extends HazelcastTestSupport {

    @Test(expected = ClassCastException.class)
    public void test_initial_map_load_propagates_exception_to_client() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Integer, Integer> map = client.getMap(mapName);
        map.get(1);

        closeResources(client, server);
    }

    private static Config createNewConfig(String mapName) {
        final ExceptionalMapStore store = new ExceptionalMapStore();
        return MapStoreTest.newConfig(mapName, store, 0);
    }

    private static class ExceptionalMapStore extends MapStoreAdapter {

        @Override
        public Set loadAllKeys() {
            final HashSet<Integer> integers = new HashSet<Integer>();
            for (int i = 0; i < 1000; i++) {
                integers.add(i);
            }
            return integers;
        }

        @Override
        public Map loadAll(Collection keys) {
            throw new ClassCastException("ExceptionalMapStore.loadAll");
        }
    }

    private static void closeResources(HazelcastInstance... instances) {
        if (instances == null) {
            return;
        }
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }
}
