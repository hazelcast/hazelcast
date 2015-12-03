package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DuplicateDetectingMultiResultTest {

    private DuplicateDetectingMultiResult result = new DuplicateDetectingMultiResult();

    @Test
    public void testAddResultSet_empty() throws Exception {
        assertThat(result.size(), is(0));
    }

    @Test
    public void testContains_empty() throws Exception {
        assertThat(result.contains(entry(data())), is(false));
    }

    @Test
    public void testIterator_empty() throws Exception {
        assertThat(result.iterator().hasNext(), is(false));
    }

    @Test
    public void testSize_empty() throws Exception {
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testAddResultSet_notEmpty() throws Exception {
        addEntry(entry(data()));

        assertThat(result.size(), is(1));
    }

    @Test
    public void testContains_notEmpty() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);

        assertThat(result.contains(entry), is(true));
    }

    @Test
    public void testIterator_notEmpty() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);

        assertThat(result.iterator().hasNext(), is(true));
        assertThat(result.iterator().next(), is(entry));
    }

    @Test
    public void testSize_notEmpty() throws Exception {
        addEntry(entry(data()));

        assertThat(result.isEmpty(), is(false));
    }

    @Test
    public void testAddResultSet_duplicate() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);
        addEntry(entry);

        assertThat(result.size(), is(1));
    }

    @Test
    public void testContains_duplicate() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);
        addEntry(entry);

        assertThat(result.contains(entry), is(true));
    }

    @Test
    public void testIterator_duplicate() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);
        addEntry(entry);

        assertThat(result.iterator().hasNext(), is(true));
        assertThat(result.iterator().next(), is(entry));
    }

    @Test
    public void testSize_duplicate() throws Exception {
        QueryableEntry entry = entry(data());
        addEntry(entry);
        addEntry(entry);

        assertThat(result.isEmpty(), is(false));
    }

    public QueryableEntry entry(Data data) {
        QueryEntry entry = mock(QueryEntry.class);
        when(entry.getKeyData()).thenReturn(data);
        return entry;
    }

    public Data data() {
        return mock(Data.class);
    }

    public void addEntry(QueryableEntry entry) {
        ConcurrentMap<Data, QueryableEntry> values = new ConcurrentHashMap<Data, QueryableEntry>();
        values.put(entry.getKeyData(), entry);
        result.addResultSet(values);
    }
}