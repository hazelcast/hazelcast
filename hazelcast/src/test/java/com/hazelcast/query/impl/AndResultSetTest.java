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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("unchecked")
public class AndResultSetTest extends HazelcastTestSupport {

    @Test
    // https://github.com/hazelcast/hazelcast/issues/1501
    public void iteratingOver_noException() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(Predicates.alwaysFalse()));
        Iterator it = resultSet.iterator();

        boolean result = it.hasNext();

        assertFalse(result);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(Predicates.alwaysFalse()));

        int size = resultSet.size();
        int countedSize = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            countedSize++;
        }

        assertEquals(0, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_notInResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        otherIndexedResults.add(Collections.<QueryableEntry>emptySet());
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            countedSize++;
        }

        assertEquals(0, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            countedSize++;
        }

        assertEquals(100000, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_matchingPredicate_inOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        Set<QueryableEntry> otherIndexResult = new HashSet<QueryableEntry>();
        otherIndexResult.add(entries.iterator().next());
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        otherIndexedResults.add(otherIndexResult);
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        int size = resultSet.size();
        int countedSize = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            countedSize++;
        }

        assertEquals(1, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    public void contains_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(Predicates.alwaysFalse()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_notInResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        otherIndexedResults.add(Collections.<QueryableEntry>emptySet());
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        for (QueryableEntry entry : entries) {
            assertContains(resultSet, entry);
        }
    }

    @Test
    public void contains_matchingPredicate_inOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        Set<QueryableEntry> otherIndexResult = new HashSet<QueryableEntry>();
        otherIndexResult.add(entries.iterator().next());
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        otherIndexedResults.add(otherIndexResult);
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(Predicates.alwaysTrue()));

        Iterator<QueryableEntry> it = entries.iterator();
        assertContains(resultSet, it.next());
        while (it.hasNext()) {
            assertNotContains(resultSet, it.next());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeUnsupported() throws IOException {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(Predicates.alwaysTrue()));

        resultSet.remove(resultSet.iterator().next());
    }

    private static Set<QueryableEntry> generateEntries(int count) {
        Set<QueryableEntry> result = new HashSet<QueryableEntry>();
        for (int k = 0; k < count; k++) {
            QueryableEntry entry = new DummyEntry();
            result.add(entry);
        }
        return result;
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
