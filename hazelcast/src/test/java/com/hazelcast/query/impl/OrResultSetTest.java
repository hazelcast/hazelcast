package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.QueryException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrResultSetTest {

    @Test
    public void size() {
        int size = 100000;
        Set<QueryableEntry> entries1 = generateEntries(size);
        Set<QueryableEntry> entries2 = generateEntries(size);
        List<Set<QueryableEntry>> indexedResults = new ArrayList<Set<QueryableEntry>>();
        indexedResults.add(entries1);
        indexedResults.add(entries2);

        OrResultSet resultSet = new OrResultSet(indexedResults);

        int sizeMethod = resultSet.size();
        int sizeIterator = 0;
        for (QueryableEntry queryableEntry : resultSet) {
            sizeIterator++;
        }

        assertEquals(sizeIterator, 2 * size);
        assertEquals(sizeMethod, 2 * size);
    }

    @Test
    public void contains() {
        int size = 100000;
        Set<QueryableEntry> entries1 = generateEntries(size);
        Set<QueryableEntry> entries2 = generateEntries(size);
        List<Set<QueryableEntry>> indexedResults = new ArrayList<Set<QueryableEntry>>();
        indexedResults.add(entries1);
        indexedResults.add(entries2);

        OrResultSet resultSet = new OrResultSet(indexedResults);
        Set<QueryableEntry> combinedEntries = new HashSet(entries1);
        combinedEntries.addAll(entries2);

        for (QueryableEntry entry : combinedEntries) {
            assertTrue(resultSet.contains(entry));
        }
        assertFalse(resultSet.contains(new DummyEntry()));
    }

    private Set<QueryableEntry> generateEntries(int count) {
        Set<QueryableEntry> result = new HashSet<QueryableEntry>();
        for (int k = 0; k < count; k++) {
            QueryableEntry entry = new DummyEntry();
            result.add(entry);
        }
        return result;
    }

    private class DummyEntry extends QueryableEntry {
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