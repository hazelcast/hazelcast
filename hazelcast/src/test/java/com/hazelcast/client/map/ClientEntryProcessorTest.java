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

import com.hazelcast.config.IndexType;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientEntryProcessorTest extends AbstractClientMapTest {

    @Test
    public void test_executeOnEntries_updatesValue_onOwnerAndBackupPartition() {
        String mapName = "test_executeOnEntries_updatesValue_onOwnerAndBackupPartition";

        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(mapName);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"));

        IMap<String, String> member1Map = member1.getMap(mapName);

        OwnerBackupValueCollector ep = new OwnerBackupValueCollector();
        member1Map.executeOnKey(member1Key, ep);

        ConcurrentLinkedQueue<String> values = OwnerBackupValueCollector.getValues();
        assertEquals(2, values.size());

        String value1 = values.poll();
        String value2 = values.poll();

        assertEquals(value1, value2);
    }

    public static class OwnerBackupValueCollector implements EntryProcessor<String, String, Object> {

        private static final ConcurrentLinkedQueue<String> values = new ConcurrentLinkedQueue<>();

        @Override
        public Object process(Map.Entry<String, String> entry) {
            values.add(entry.getValue());
            return null;
        }

        public static ConcurrentLinkedQueue<String> getValues() {
            return values;
        }
    }

    @Test
    public void test_executeOnEntries_notUpdatesValue_with_FalsePredicate() {
        String mapName = "test_executeOnEntries_notUpdatesValue_with_FalsePredicate";

        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(mapName);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"), Predicates.alwaysFalse());

        IMap<String, String> member1Map = member1.getMap(mapName);
        String member1Value = member1Map.get(member1Key);

        assertEquals("value", member1Value);
    }

    @Test
    public void test_executeOnEntries_updatesValue_with_TruePredicate() {
        String mapName = "test_executeOnEntries_updatesValue_with_TruePredicate";

        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(mapName);
        clientMap.put(member1Key, "value");

        clientMap.executeOnEntries(new ValueUpdater("newValue"), Predicates.alwaysTrue());

        IMap<String, String> member1Map = member1.getMap(mapName);
        String member1Value = member1Map.get(member1Key);

        assertEquals("newValue", member1Value);
    }

    @Test
    public void test_executeOnEntriesWithPredicate_usesIndexes_whenIndexesAvailable() {
        String mapName = "test_executeOnEntriesWithPredicate_usesIndexes_whenIndexesAvailable";

        IMap<Integer, Integer> map = client.getMap(mapName);
        map.addIndex(IndexType.SORTED, "__key");

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        IndexedTestPredicate predicate = new IndexedTestPredicate();
        map.executeOnEntries(new EP(), predicate);

        assertTrue("isIndexed method of IndexAwarePredicate should be called", IndexedTestPredicate.INDEX_CALLED.get());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_executeOnKey_readOnly_setValue() {
        String mapName = "test_executeOnKey_readOnly_setValue";

        String member1Key = generateKeyOwnedBy(member1);

        IMap<String, String> clientMap = client.getMap(mapName);
        clientMap.put(member1Key, "value");

        clientMap.executeOnKey(member1Key, new ValueUpdaterReadOnly("newValue"));
    }

    public static final class EP implements EntryProcessor {

        @Override
        public Object process(Map.Entry entry) {
            return null;
        }
    }

    /**
     * This predicate is used to check whether or not {@link IndexAwarePredicate#isIndexed} method is called.
     */
    private static class IndexedTestPredicate implements IndexAwarePredicate {

        static final AtomicBoolean INDEX_CALLED = new AtomicBoolean(false);

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

    public static class ValueUpdater<K, R> implements EntryProcessor<K, String, R> {

        private final String newValue;

        ValueUpdater(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public R process(Map.Entry<K, String> entry) {
            entry.setValue(newValue);
            return null;
        }
    }

    public static class ValueUpdaterReadOnly<K, R> implements EntryProcessor<K, String, R>, ReadOnly {

        private final String newValue;

        ValueUpdaterReadOnly(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public R process(Map.Entry<K, String> entry) {
            entry.setValue(newValue);
            return null;
        }

        @Override
        public EntryProcessor<K, String, R> getBackupProcessor() {
            return null;
        }
    }
}
