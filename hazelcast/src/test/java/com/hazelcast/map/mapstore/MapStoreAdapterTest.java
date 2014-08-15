
/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map.mapstore;


import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreAdapterTest extends HazelcastTestSupport {

    @Test
    public void testStoreAll() {
        MockMapStoreAdapter adapter = new MockMapStoreAdapter();
        Map map = new HashMap();
        map.put(1, 1);
        map.put(2, 2);

        adapter.storeAll(map);

        assertEquals(2, adapter.stored.size());
    }

    @Test
    public void testLoadAll() {
        MockMapStoreAdapter adapter = new MockMapStoreAdapter();
        adapter.store(1, 1);
        adapter.store(2, 2);
        Collection<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(2);

        Map map = adapter.loadAll(keySet);

        assertEquals(adapter.loaded.size(), map.size());
    }

    @Test
    public void testDeleteAll() {
        MockMapStoreAdapter adapter = new MockMapStoreAdapter();
        adapter.store(1, 1);
        adapter.store(2, 2);
        Collection<Integer> keySet = new HashSet<Integer>();
        keySet.add(1);
        keySet.add(2);

        adapter.deleteAll(keySet);

        assertEquals(2, adapter.deleted.size());
    }

    private class MockMapStoreAdapter extends MapStoreAdapter {
        private List deleted;
        private List stored;
        private List loaded;

        MockMapStoreAdapter() {
            deleted = new LinkedList();
            stored = new LinkedList();
            loaded = new LinkedList();
        }

        public void delete(Object key) {
            deleted.add(key);
        }

        public void store(Object key, Object value) {
            stored.add(new Object[]{key, value});
        }

        public void load(Object key, Object value) {
            loaded.add(new Object[]{key, value});
        }

        public Set loadAllKeys() {
            return super.loadAllKeys();
        }

    }
}
