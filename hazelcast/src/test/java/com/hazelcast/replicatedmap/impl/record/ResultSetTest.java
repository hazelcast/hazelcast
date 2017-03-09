/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
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
@Category({QuickTest.class, ParallelTest.class})
public class ResultSetTest {

    @Test
    public void testSize_whenEmpty() throws Exception {
        List<Map.Entry<Object, Object>> emptyList = Collections.emptyList();
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(emptyList, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNull() throws Exception {
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(null, IterationType.KEY);
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testSize_whenNotEmpty() throws Exception {
        List<Map.Entry<Object, Object>> entries = new ArrayList<Map.Entry<Object, Object>>();
        entries.add(new MapEntrySimple<Object, Object>(null, null));
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(entries, IterationType.KEY);
        assertEquals(1, resultSet.size());
    }

    @Test
    public void testIterator_whenEmpty() throws Exception {
        List<Map.Entry<Object, Object>> emptyList = Collections.emptyList();
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(emptyList, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNull() throws Exception {
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(null, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Key() throws Exception {
        List<Map.Entry<Object, Object>> entries = new ArrayList<Map.Entry<Object, Object>>();
        MapEntrySimple<Object, Object> entry = new MapEntrySimple<Object, Object>("key", "value");
        entries.add(entry);
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(entries, IterationType.KEY);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("key", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Value() throws Exception {
        List<Map.Entry<Object, Object>> entries = new ArrayList<Map.Entry<Object, Object>>();
        MapEntrySimple<Object, Object> entry = new MapEntrySimple<Object, Object>("key", "value");
        entries.add(entry);
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(entries, IterationType.VALUE);
        Iterator iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("value", iterator.next());
    }

    @Test
    public void testIterator_whenNotEmpty_IterationType_Entry() throws Exception {
        List<Map.Entry<Object, Object>> entries = new ArrayList<Map.Entry<Object, Object>>();
        MapEntrySimple<Object, Object> entry = new MapEntrySimple<Object, Object>("key", "value");
        entries.add(entry);
        ResultSet<Object, Object> resultSet = new ResultSet<Object, Object>(entries, IterationType.ENTRY);
        Iterator<Map.Entry<Object, Object>> iterator = resultSet.iterator();
        assertTrue(iterator.hasNext());
        Map.Entry<Object, Object> entryFromIterator = iterator.next();
        assertEquals("key", entryFromIterator.getKey());
        assertEquals("value", entryFromIterator.getValue());

    }


}
