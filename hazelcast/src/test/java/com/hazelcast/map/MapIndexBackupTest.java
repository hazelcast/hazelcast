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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexBackupTest extends HazelcastTestSupport {

    // Issue: https://github.com/hazelcast/hazelcast/issues/6840
    @Test
    public void backupsShouldNotBeIndexedWhenThereIsNoMigration() {
        backupsShouldNotBeIndexed(false);
    }

    // Issue: https://github.com/hazelcast/hazelcast/issues/6840
    @Test
    public void backupsShouldNotBeIndexedWhenThereIsMigration() {
        backupsShouldNotBeIndexed(true);
    }

    private void backupsShouldNotBeIndexed(boolean migrationHappens) {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = createNode(instanceFactory);

        if (!migrationHappens) {
            createNode(instanceFactory);
        }

        IMap bookMap = instance1.getMap("book");
        bookMap.loadAll(true);

        if (migrationHappens) {
            HazelcastInstance instance2 = createNode(instanceFactory);
            waitAllForSafeState(instance1, instance2);
        }

        Set<Object> foundByPredicate = new TreeSet<Object>(
                bookMap.localKeySet(
                        Predicates.and(
                                Predicates.in("author", "0", "1", "2", "3", "4", "5", "6"),
                                Predicates.between("year", 1990, 2000))));

        Map<Member, Set<Object>> foundByPredicateByMember = new HashMap<Member, Set<Object>>();
        for (Object key : foundByPredicate) {
            Member owner = instance1.getPartitionService().getPartition(key).getOwner();
            Set<Object> keys = foundByPredicateByMember.get(owner);
            if (keys == null) {
                keys = new HashSet<Object>();
                foundByPredicateByMember.put(owner, keys);
            }
            keys.add(key);
        }

        assertEquals(1, foundByPredicateByMember.size());
        assertTrue(foundByPredicateByMember.keySet().iterator().next().localMember());
    }

    private HazelcastInstance createNode(TestHazelcastInstanceFactory instanceFactory) {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("book");
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "author"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "year"));
        mapConfig.setMapStoreConfig(new MapStoreConfig().setImplementation(new BookMapLoader()));
        mapConfig.setBackupCount(1);
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
            List<Integer> keys = new ArrayList<Integer>(2000);
            for (int i = 0; i < 2000; i++) {
                keys.add(i);
            }
            return keys;
        }
    }
}
