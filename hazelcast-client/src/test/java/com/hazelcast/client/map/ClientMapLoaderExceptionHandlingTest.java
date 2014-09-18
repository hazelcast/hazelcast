package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.mapstore.MapStoreTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLoaderExceptionHandlingTest extends HazelcastTestSupport {

    private static final String mapName = randomMapName();
    private static HazelcastInstance client;
    private static HazelcastInstance server;

    @BeforeClass
    public static void init() {
        final Config config = createNewConfig(mapName);
        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test_initial_map_load_propagates_exception_to_client() throws Exception {
        final IMap<Integer, Integer> map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Exception exception = null;
                try {
                    map.get(1);
                } catch (Exception e) {
                    exception = e;
                }
                assertNotNull(exception);
                assertEquals(ClassCastException.class, exception.getClass());

            }
        });
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
