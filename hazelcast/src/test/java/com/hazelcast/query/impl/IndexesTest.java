/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import static com.hazelcast.instance.TestUtil.toData;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.Value;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IndexesTest {

    final SerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testAndWithSingleEntry() throws Exception {
        Indexes indexes = new Indexes(ss);
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);
        for (int i = 0; i < 20000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            indexes.saveEntryIndex(new QueryEntry(ss, toData(i), i, employee), null);
        }
        int count = 1000;
        Set<String> ages = new HashSet<String>(count);
        for (int i = 0; i < count; i++) {
            ages.add(String.valueOf(i));
        }
        final EntryObject entryObject = new PredicateBuilder().getEntryObject();
        final PredicateBuilder predicate = entryObject.get("name").equal("140Name").and(entryObject.get("age").in(ages.toArray(new String[0])));
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        long start = Clock.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            Set<QueryableEntry> results = indexes.query(predicate);
            assertEquals(1, results.size());
        }
    }

    @Test
    public void testIndex() throws Exception {
        Indexes indexes = new Indexes(ss);
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);
        for (int i = 0; i < 2000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 100));
            indexes.saveEntryIndex(new QueryEntry(ss, toData(i), i, employee), null);
        }

        for (int i = 0; i < 10; i++) {
            SqlPredicate predicate = new SqlPredicate("salary=161 and age >20 and age <23");
            Set<QueryableEntry> results = new HashSet<QueryableEntry>(indexes.query(predicate));
            assertEquals(5, results.size());
        }
    }

    @Test
    public void testIndex2() throws Exception {
        Indexes indexes = new Indexes(ss);
        indexes.addOrGetIndex("name", false);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(1), 1, new Value("abc")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(2), 2, new Value("xyz")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(3), 3, new Value("aaa")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(4), 4, new Value("zzz")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(5), 5, new Value("klm")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(6), 6, new Value("prs")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(7), 7, new Value("prs")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(8), 8, new Value("def")), null);
        indexes.saveEntryIndex(new QueryEntry(ss, toData(9), 9, new Value("qwx")), null);
        assertEquals(8, new HashSet(indexes.query(new SqlPredicate("name > 'aac'"))).size());
    }


    /**
     * Imagine we have only keys and nullable values. And we add index for a field of that nullable object.
     * When we execute a query on keys, there should be no returned value from indexing service and it does not
     * throw exception.
     */
    @Test
    public void shouldNotThrowException_withNullValues_whenIndexAddedForValueField() throws Exception {
        Indexes indexes = new Indexes(ss);
        indexes.addOrGetIndex("name", false);

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }


    @Test
    public void shouldNotThrowException_withNullValues_whenNoIndexAdded() throws Exception {
        Indexes indexes = new Indexes(ss);

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }

    private void shouldReturnNull_whenQueryingOnKeys(Indexes indexes) {
        for (int i = 0; i < 50; i++) {
            // passing null value to QueryEntry.
            indexes.saveEntryIndex(new QueryEntry(ss, toData(i), i, null), null);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertNull("There should be no result", query);
    }

    @Test
    public void shouldNotThrowException_withNullValue_whenIndexAddedForKeyField() throws Exception {
        Indexes indexes = new Indexes(ss);
        indexes.addOrGetIndex("__key", false);

        for (int i = 0; i < 100; i++) {
            // passing null value to QueryEntry.
            indexes.saveEntryIndex(new QueryEntry(ss, toData(i), i, null), null);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertEquals(89, query.size());
    }
}
