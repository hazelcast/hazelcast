/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FastMultiResultSetTest {

    private final FastMultiResultSet result = new FastMultiResultSet();

    @Test
    public void testAddResultSet_empty() {
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testContains_empty() {
        assertThat(result.contains(entry(data()))).isFalse();
    }

    @Test
    public void testIterator_empty() {
        assertThat(result.iterator().hasNext()).isFalse();
    }

    @Test
    public void testSize_empty() {
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testAddResultSet_notEmpty() {
        addEntry(entry(data()));

        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void testContains_notEmpty() {
        QueryableEntry entry = entry(data());
        addEntry(entry);

        assertThat(result.contains(entry)).isTrue();
    }

    @Test
    public void testIterator_notEmpty() {
        QueryableEntry entry = entry(data());
        addEntry(entry);

        assertThat(result.iterator().hasNext()).isTrue();
        assertThat(result.iterator().next()).isEqualTo(entry);
    }

    @Test
    public void testIterator_notEmpty_iteratorReused() {
        QueryableEntry entry = entry(data());
        addEntry(entry);

        Iterator<QueryableEntry> it = result.iterator();
        assertThat(it.hasNext()).isTrue();
        assertThat(it.next()).isEqualTo(entry);
    }

    @Test
    public void testIterator_empty_next() {
        assertNull(result.iterator().next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_empty_remove() {
        result.iterator().remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_addUnsopperted() {
        result.add(mock(QueryableEntry.class));
    }

    @Test
    public void testSize_notEmpty() {
        addEntry(entry(data()));

        assertThat(result.isEmpty()).isFalse();
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
        ConcurrentMap<Data, QueryableEntry> values = new ConcurrentHashMap<>();
        values.put(entry.getKeyData(), entry);
        result.addResultSet(values);
    }
}
