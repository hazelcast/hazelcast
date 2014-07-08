package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapPredicateTest extends HazelcastTestSupport {

    @Test
    public void testMapKeySet() {
        IMap<String, String> map = getMapWithNodeCount(3);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        List<String> listExpected = new ArrayList<String>();
        listExpected.add("key1");
        listExpected.add("key2");
        listExpected.add("key3");

        final List<String> list = new ArrayList<String>(map.keySet());

        Collections.sort(list);
        Collections.sort(listExpected);

        assertEquals(listExpected, list);
    }

    @Test
    public void testMapLocalKeySet() {
        IMap<String, String> map = getMapWithNodeCount(3);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        List<String> listExpected = new ArrayList<String>();
        listExpected.add("key1");
        listExpected.add("key2");
        listExpected.add("key3");

        final List<String> list = new ArrayList<String>(map.keySet());

        Collections.sort(list);
        Collections.sort(listExpected);

        assertEquals(listExpected, list);
    }

    @Test
    public void testMapValues() {
        IMap<String, String> map = getMapWithNodeCount(3);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value3");
        List<String> values = new ArrayList<String>(map.values());
        List<String> expected = new ArrayList<String>();
        expected.add("value1");
        expected.add("value2");
        expected.add("value3");
        expected.add("value3");
        Collections.sort(values);
        Collections.sort(expected);
        assertEquals(expected, values);
    }

    @Test
    public void valuesToArray() {
        IMap<String, String> map = getMapWithNodeCount(3);
        assertEquals(0, map.size());
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        assertEquals(3, map.size());
        {
            final Object[] values = map.values().toArray();
            Arrays.sort(values);
            assertArrayEquals(new Object[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = map.values().toArray(new String[3]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = map.values().toArray(new String[2]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = map.values().toArray(new String[5]);
            Arrays.sort(values, 0, 3);
            assertArrayEquals(new String[]{"1", "2", "3", null, null}, values);
        }
    }

    @Test
    public void testMapEntrySetWhenRemoved() {
        IMap<String, String> map = getMapWithNodeCount(3);
        map.put("Hello", "World");
        map.remove("Hello");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            fail("Iterator should not contain removed entry, found " + e.getKey());
        }
    }

    @Test
    public void testEntrySet() {
        final IMap<Object, Object> map = getMapWithNodeCount(3);
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        map.put(5, 5);
        Set<Map.Entry> entrySet = new HashSet<Map.Entry>();
        entrySet.add(new AbstractMap.SimpleImmutableEntry(1, 1));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(2, 2));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(3, 3));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(4, 4));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(5, 5));
        assertEquals(entrySet, map.entrySet());
    }

    private IMap getMapWithNodeCount(int nodeCount) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance node = factory.newHazelcastInstance();
        return node.getMap(randomMapName());
    }

}
