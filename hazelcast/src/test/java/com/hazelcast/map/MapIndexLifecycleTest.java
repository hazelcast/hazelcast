/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapIndexLifecycleTest extends HazelcastTestSupport {

    private static final int BOOK_COUNT = 1000;

    @Test
    public void recordStoresAndIndexes_createdDestroyedProperly() {
        // GIVEN
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = createNode(instanceFactory);

        // THEN - initialized
        IMap bookMap = instance1.getMap("default");
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);

        // THEN - destroyed
        bookMap.destroy();
        assertAllPartitionContainersAreEmpty(instance1);

        // THEN - initialized
        bookMap = instance1.getMap("default");
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);
    }

    private void assertAllPartitionContainersAreEmpty(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);

        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);

            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            assertTrue("record stores not empty", maps.isEmpty());

            ConcurrentMap<String, Indexes> indexes = container.getIndexes();
            assertTrue("indexes not empty", indexes.isEmpty());
        }
    }

    private void assertAllPartitionContainersAreInitialized(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);

        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);

            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            RecordStore recordStore = maps.get("default");
            assertNotNull("record store is null", recordStore);

            if (context.getMapContainer("default").getMapConfig().getInMemoryFormat().equals(NATIVE)) {
                ConcurrentMap<String, Indexes> indexes = container.getIndexes();
                Indexes index = indexes.get("default");
                assertNotNull("indexes is null", index);
            }
        }
    }

    private int getPartitionCount(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.getProperties().getInteger(GroupProperty.PARTITION_COUNT);
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }


    private HazelcastInstance createNode(TestHazelcastInstanceFactory instanceFactory) {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.addMapIndexConfig(new MapIndexConfig("author", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("year", true));
        mapConfig.setBackupCount(1);
        mapConfig.setMapStoreConfig(new MapStoreConfig().setImplementation(new BookMapLoader()));
        return instanceFactory.newHazelcastInstance(config);
    }

    public static class Book implements Serializable {

        private long id;
        private String title;
        private String author;
        private int year;

        private Book() {
        }

        Book(long id, String title, String author, int year) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.year = year;
        }

        public long getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getAuthor() {
            return author;
        }

        public int getYear() {
            return year;
        }
    }

    private static class BookMapLoader implements MapLoader<Integer, Book> {
        @Override
        public Book load(Integer key) {
            return loadAll(Collections.singleton(key)).get(key);
        }

        @Override
        public Map<Integer, Book> loadAll(Collection<Integer> keys) {
            Map<Integer, Book> map = new TreeMap<Integer, Book>();
            for (int key : keys) {
                map.put(key, new Book(key, String.valueOf(key), String.valueOf(key % 7), 1800 + key % 200));
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            List<Integer> keys = new ArrayList<Integer>(BOOK_COUNT);
            for (int i = 0; i < BOOK_COUNT; i++) {
                keys.add(i);
            }
            return keys;
        }
    }

}
