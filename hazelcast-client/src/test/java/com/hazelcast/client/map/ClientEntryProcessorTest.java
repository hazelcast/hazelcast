/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientEntryProcessorTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    private HazelcastInstance client;

    private HazelcastInstance member1;
    private HazelcastInstance member2;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();

        TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
        member1 = hazelcastFactory.newHazelcastInstance(config);
        member2 = hazelcastFactory.newHazelcastInstance(config);

        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() throws Exception {
        client.shutdown();

        member1.shutdown();
        member2.shutdown();
    }

    @Test
    public void test_executeOnEntries_updatesValue_onOwnerAndBackupPartition() {
        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(MAP_NAME);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"));

        IMap<String, String> member1Map = member1.getMap(MAP_NAME);
        String member1Value = member1Map.get(member1Key);

        member1.shutdown();

        IMap<String, String> member2Map = member2.getMap(MAP_NAME);
        String member2Value = member2Map.get(member1Key);

        assertEquals("newValue", member1Value);
        assertEquals("newValue", member2Value);
    }

    @Test
    public void test_executeOnEntries_notUpdatesValue_with_FalsePredicate() {
        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(MAP_NAME);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"), FalsePredicate.INSTANCE);

        IMap<String, String> member1Map = member1.getMap(MAP_NAME);
        String member1Value = member1Map.get(member1Key);

        assertEquals("value", member1Value);
    }


    @Test
    public void test_executeOnEntries_updatesValue_with_TruePredicate() {
        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(MAP_NAME);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"), TruePredicate.INSTANCE);

        IMap<String, String> member1Map = member1.getMap(MAP_NAME);
        String member1Value = member1Map.get(member1Key);

        assertEquals("newValue", member1Value);
    }


    @Test
    public void test_executeOnEntriesWithPredicate_usesIndexes_whenIndexesAvailable() {
        IMap<Integer, Integer> map = client.getMap("test");
        map.addIndex("__key", true);

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        IndexedTestPredicate predicate = new IndexedTestPredicate();
        map.executeOnEntries(new EP(), predicate);


        assertTrue("isIndexed method of IndexAwarePredicate should be called", IndexedTestPredicate.INDEX_CALLED.get());
    }

    public static final class EP extends AbstractEntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            return null;
        }
    }


    /**
     * This predicate is used to check whether or not {@link IndexAwarePredicate#isIndexed} method is called.
     */
    private static class IndexedTestPredicate implements IndexAwarePredicate {

        public static final AtomicBoolean INDEX_CALLED = new AtomicBoolean(false);

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            INDEX_CALLED.set(true);
            return true;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }


    public static class ValueUpdater extends AbstractEntryProcessor {

        private final String newValue;

        public ValueUpdater(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public Object process(Map.Entry entry) {
            entry.setValue(newValue);
            return null;
        }
    }






}
