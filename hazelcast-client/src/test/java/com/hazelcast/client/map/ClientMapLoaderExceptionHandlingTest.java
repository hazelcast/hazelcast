package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.mapstore.MapStoreTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.ValidationUtil.checkState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLoaderExceptionHandlingTest extends HazelcastTestSupport {

    private static final String mapName = randomMapName();
    private HazelcastInstance client;
    private HazelcastInstance server;
    private ExceptionalMapStore mapStore;

    @Before
    public void init() {
        mapStore = new ExceptionalMapStore();
        Config config = createNewConfig(mapName, mapStore);
        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @After
    public void shutdown() {
        closeResources(client, server);
    }

    @Before
    public void configureMapStore() {
        mapStore.setLoadAllKeysThrows(false);
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
                assertNotNull("Exception not propagated to client", exception);
                assertEquals(ClassCastException.class, exception.getClass());
            }
        });
    }

    @Test
    public void testClientGetsException_whenLoadAllKeysThrowsOne() throws Exception {
        mapStore.setLoadAllKeysThrows(true);

        IMap<Integer, Integer> map = client.getMap(mapName);

        Exception exception = null;
        try {
            map.get(1);
        } catch (Exception e) {
            exception = e;
        }

        assertNotNull("Exception not propagated to client", exception);
        assertEquals(IllegalStateException.class, exception.getClass());
    }

    private static Config createNewConfig(String mapName, MapStore store) {
        return MapStoreTest.newConfig(mapName, store, 0);
    }

    private static class ExceptionalMapStore extends MapStoreAdapter {

        private boolean loadAllKeysThrows = false;

        @Override
        public Set loadAllKeys() {
            checkState(!loadAllKeysThrows, getClass().getName());

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

        public void setLoadAllKeysThrows(boolean loadAllKeysThrows) {
            this.loadAllKeysThrows = loadAllKeysThrows;
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
