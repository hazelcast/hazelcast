/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapEntrySetTest extends HazelcastTestSupport {

    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance instance = createHazelcastInstance();

        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.entrySet(null);
    }

    @Test
    public void whenMapEmpty() {
        Set<Map.Entry<String, String>> result = map.entrySet(Predicates.alwaysTrue());
        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<Map.Entry<String, String>> result = map.entrySet();

        assertEquals(3, result.size());
        assertResultContains(result, "1", "a");
        assertResultContains(result, "2", "b");
        assertResultContains(result, "3", "c");
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<Map.Entry<String, String>> result = map.entrySet(Predicates.alwaysTrue());

        assertEquals(3, result.size());
        assertResultContains(result, "1", "a");
        assertResultContains(result, "2", "b");
        assertResultContains(result, "3", "c");
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Set<Map.Entry<String, String>> result = map.entrySet(new GoodPredicate());

        assertEquals(2, result.size());
        assertResultContains(result, "1", "good1");
        assertResultContains(result, "3", "good2");
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Set<Map.Entry<String, String>> result = map.entrySet(Predicates.alwaysTrue());

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, result);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        assertEquals(serializationService.toData("1"), row.getKey());
        assertEquals(serializationService.toData("a"), row.getValue());
    }

    private static void assertResultContains(Set<Map.Entry<String, String>> result, String key, String value) {
        assertContains(result, new SimpleEntry<String, String>(key, value));
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }

    }
}
