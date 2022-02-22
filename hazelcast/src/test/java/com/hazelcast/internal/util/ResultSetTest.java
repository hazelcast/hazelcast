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

package com.hazelcast.internal.util;

import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class ResultSetTest {

    @Test
    public void testSize_whenEmpty() {
        List<Map.Entry> emptyList = Collections.emptyList();
        ResultSet resultSet = new ResultSet(emptyList, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNull() {
        ResultSet resultSet = new ResultSet(null, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNotEmpty() {
        List<Map.Entry> entries = new ArrayList<Map.Entry>();
        entries.add(new MapEntrySimple(null, null));
        ResultSet resultSet = new ResultSet(entries, IterationType.KEY);
        assertEquals(1, resultSet.size());
    }

    @Test
    public void testIterator_whenEmpty() {
        List<Map.Entry> emptyList = Collections.emptyList();
        ResultSet resultSet = new ResultSet(emptyList, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNull() {
        ResultSet resultSet = new ResultSet(null, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Key() {
        List<Map.Entry> entries = new ArrayList<Map.Entry>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("key", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Value() {
        List<Map.Entry> entries = new ArrayList<Map.Entry>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.VALUE);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("value", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Entry() {
        List<Map.Entry> entries = new ArrayList<Map.Entry>();
        MapEntrySimple entry = new MapEntrySimple("key", "value");
        entries.add(entry);
        ResultSet resultSet = new ResultSet(entries, IterationType.ENTRY);
        Iterator<Map.Entry> iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        Map.Entry entryFromIterator = iterator.next();
        assertEquals("key", entryFromIterator.getKey());
        assertEquals("value", entryFromIterator.getValue());
    }
}
