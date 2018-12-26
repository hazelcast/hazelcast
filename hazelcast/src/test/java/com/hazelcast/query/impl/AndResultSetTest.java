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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unchecked")
public class AndResultSetTest extends HazelcastTestSupport {

    @Test
    // https://github.com/hazelcast/hazelcast/issues/1501
    public void iteratingOver_noException() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, asList(new FalsePredicate()));
        resultSet.init();
        assertFalse(resultSet.iterator().hasNext());
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/9614
    public void size_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, asList(new FalsePredicate()));
        resultSet.init();
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
        AndResultSet resultSet = new AndResultSet(entries, asList(new TruePredicate()));
        resultSet.init();
        int size = resultSet.size();
        int countedSize = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            countedSize++;
        }

        assertEquals(100000, countedSize);
        assertEquals(size, countedSize);
    }

    @Test
    public void contains_nonMatchingPredicate() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, asList(new FalsePredicate()));

        assertNotContains(resultSet, entries.iterator().next());
    }

    @Test
    public void contains_matchingPredicate_noOtherResult() {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, asList(new TruePredicate()));
        resultSet.init();
        for (QueryableEntry entry : entries) {
            assertContains(resultSet, entry);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeUnsupported() throws IOException {
        Set<QueryableEntry> entries = generateEntries(100000);
        AndResultSet resultSet = new AndResultSet(entries, asList(new TruePredicate()));
        resultSet.init();
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
