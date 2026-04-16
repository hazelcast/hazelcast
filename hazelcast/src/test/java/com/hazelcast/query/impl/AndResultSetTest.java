/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("rawtypes")
public class AndResultSetTest extends HazelcastTestSupport {

    @Test
    // https://github.com/hazelcast/hazelcast/issues/1501
    public void iteratingOver_noException() {
        Set<QueryableEntry> entries = generateEntries();
        AndResultSet resultSet = new AndResultSet(entries, null, singletonList(Predicates.alwaysFalse()));
        Iterator it = resultSet.iterator();

        boolean result = it.hasNext();

        assertFalse(result);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries();
        AndResultSet resultSet = new AndResultSet(entries, null, singletonList(Predicates.alwaysFalse()));

        int size = resultSet.size();
        int countedSize = countItems(resultSet);

        assertEquals(0, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_notInResult() {
        Set<QueryableEntry> entries = generateEntries();
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        otherIndexedResults.add(Collections.emptySet());
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = countItems(resultSet);

        assertEquals(0, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries();
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = countItems(resultSet);

        assertEquals(100000, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_inOtherResult() {
        Set<QueryableEntry> entries = generateEntries();
        Set<QueryableEntry> otherIndexResult = new HashSet<>();
        otherIndexResult.add(entries.iterator().next());
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        otherIndexedResults.add(otherIndexResult);
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = countItems(resultSet);

        assertEquals(1, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    public void contains_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries();
        AndResultSet resultSet = new AndResultSet(entries, null, singletonList(Predicates.alwaysFalse()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_notInResult() {
        Set<QueryableEntry> entries = generateEntries();
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        otherIndexedResults.add(Collections.emptySet());
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries();
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        for (QueryableEntry entry : entries) {
            assertContains(resultSet, entry);
        }
    }

    @Test
    public void contains_matchingPredicate_inOtherResult() {
        Set<QueryableEntry> entries = generateEntries();
        Set<QueryableEntry> otherIndexResult = new HashSet<>();
        otherIndexResult.add(entries.iterator().next());
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<>();
        otherIndexedResults.add(otherIndexResult);
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, singletonList(Predicates.alwaysTrue()));

        Iterator<QueryableEntry> it = entries.iterator();
        assertContains(resultSet, it.next());
        while (it.hasNext()) {
            assertNotContains(resultSet, it.next());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeUnsupported() {
        Set<QueryableEntry> entries = generateEntries();
        AndResultSet resultSet = new AndResultSet(entries, null, singletonList(Predicates.alwaysTrue()));

        resultSet.remove(resultSet.iterator().next());
    }

    private static Set<QueryableEntry> generateEntries() {
        Set<QueryableEntry> result = new HashSet<>();
        for (int k = 0; k < 100000; k++) {
            QueryableEntry entry = new DummyEntry();
            result.add(entry);
        }
        return result;
    }

    private int countItems(Iterable<?> resultSet) {
        int count = 0;
        for (var iter = resultSet.iterator(); iter.hasNext(); iter.next()) {
            count++;
        }
        return count;
    }

    private static class DummyEntry extends QueryableEntry {

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Comparable getAttributeValue(String attributeName) throws QueryException {
            return null;
        }

        @Override
        public Object getTargetObject(boolean key) {
            return null;
        }

        @Override
        public Object setValue(Object value) {
            return null;
        }

        @Override
        public Data getKeyData() {
            return null;
        }

        @Override
        public Data getValueData() {
            return null;
        }

        @Override
        public Object getKeyIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getKeyDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Object getValueIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getValueDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }
    }
}
