/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unchecked")
public class AndResultSetTest extends HazelcastTestSupport {

    @Test
    // https://github.com/hazelcast/hazelcast/issues/1501
    public void iteratingOver_noException() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(new FalsePredicate()));
        Iterator it = resultSet.iterator();

        boolean result = it.hasNext();

        assertFalse(result);
    }

    @Test
    public void contains_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(new FalsePredicate()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_notInResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        otherIndexedResults.add(Collections.<QueryableEntry>emptySet());
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(new TruePredicate()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        List<Set<QueryableEntry>> otherIndexedResults = new ArrayList<Set<QueryableEntry>>();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(new TruePredicate()));

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
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(new TruePredicate()));

        Iterator<QueryableEntry> it = entries.iterator();
        assertContains(resultSet, it.next());
        while (it.hasNext()) {
            assertNotContains(resultSet, it.next());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeUnsupported() throws IOException {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, null, asList(new TruePredicate()));

        resultSet.remove(resultSet.iterator().next());
    }

    @Test
    public void is_empty_returns_true_when_result_set_has_no_entry() {
        Set<QueryableEntry> entries = Collections.emptySet();
        List<Set<QueryableEntry>> otherIndexedResults = Collections.emptyList();
        AndResultSet resultSet = new AndResultSet(entries, otherIndexedResults, asList(new TruePredicate()));

        assertTrue(resultSet.isEmpty());
    }

    @Test
    public void is_empty_returns_false_when_result_set_has_entries() {
        // 1. prepare matchingEntries
        Set<QueryableEntry> matchingEntries = generateEntries(100);

        // 2. prepare list of matchingEntries from indexes
        Set<QueryableEntry> indexSearchResult = new HashSet<QueryableEntry>();
        int count = 0;
        for (QueryableEntry entry : matchingEntries) {
            if (count == 10) {
                break;
            }
            indexSearchResult.add(entry);
            count++;
        }
        List<Set<QueryableEntry>> listOfIndexSearchResults
                = new LinkedList<Set<QueryableEntry>>(singletonList(indexSearchResult));

        // 3. add all results to result set
        AndResultSet resultSet = new AndResultSet(matchingEntries,
                listOfIndexSearchResults, asList(new TruePredicate()));

        assertFalse(resultSet.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void size_throws_unsupported_operation_exception() {
        AndResultSet resultSet = new AndResultSet(Collections.<QueryableEntry>emptySet(),
                Collections.<Set<QueryableEntry>>emptyList(), asList(new TruePredicate()));

        resultSet.size();
    }

    private static Set<QueryableEntry> generateEntries(int count) {
        Set<QueryableEntry> result = new HashSet<QueryableEntry>();
        for (int k = 0; k < count; k++) {
            QueryableEntry entry = new DummyEntry();
            result.add(entry);
        }
        return result;
    }

    private static class FalsePredicate implements Predicate {

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
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
        public AttributeType getAttributeType(String attributeName) {
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
    }
}
