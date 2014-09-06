package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.codehaus.groovy.runtime.InvokerHelper.asList;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AndResultSetTest extends HazelcastTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/1501
    //tests that this method is not running into a stackoverflow.
    @Test
    public void issue_1501() {
        Set<QueryableEntry> entries = generateEntries(100000);
        System.out.println(entries.size());
        AndResultSet resultSet = new AndResultSet(entries, null, asList(new FalsePredicate()));
        Iterator it = resultSet.iterator();
        boolean result = it.hasNext();
        assertFalse(result);
    }

    //todo: we need to have more methods for regular behavior.

    class FalsePredicate implements Predicate {
        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }

    private Set<QueryableEntry> generateEntries(int count) {
        Set<QueryableEntry> result = new HashSet<QueryableEntry>();
        for (int k = 0; k < count; k++) {
            QueryableEntry entry = new DummyEntry();
            result.add(entry);
        }
        return result;
    }

    private class DummyEntry implements QueryableEntry {
        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Comparable getAttribute(String attributeName) throws QueryException {
            return null;
        }

        @Override
        public AttributeType getAttributeType(String attributeName) {
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
        public Data getIndexKey() {
            return null;
        }
    }
}
