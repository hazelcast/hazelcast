package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
