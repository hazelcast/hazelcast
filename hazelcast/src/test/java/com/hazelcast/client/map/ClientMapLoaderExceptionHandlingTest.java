/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.mapstore.AbstractMapStoreTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapLoaderExceptionHandlingTest extends AbstractMapStoreTest {

    private static final String mapName = randomMapName();
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private ExceptionalMapStore mapStore;

    @Before
    public void setup() {
        mapStore = new ExceptionalMapStore();
        Config config = createNewConfig(mapName, mapStore);
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
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

    private Config createNewConfig(String mapName, MapStore store) {
        return newConfig(mapName, store, 0);
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
}
