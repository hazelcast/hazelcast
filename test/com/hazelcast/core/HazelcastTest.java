package com.hazelcast.core;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;

public class HazelcastTest {

    @Test
    public void testMapGetName(){
        IMap<String,String> map = Hazelcast.getMap("testMapGetName");
        assertEquals("testMapGetName", map.getName());
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String,String> map = Hazelcast.getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1,map.size());
        assertNull(value);

        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1,map.size());
        assertEquals("World", value);

        value = map.put("Hello", "New World");
        assertEquals("New World", map.get("Hello"));
        assertEquals(1,map.size());
        assertEquals("World", value);
    }

    @Test
    public void testMapContainsKey(){
        IMap<String,String> map = Hazelcast.getMap("testMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMapContainsValue(){
        IMap<String,String> map = Hazelcast.getMap("testMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMapClear(){
        IMap<String,String> map = Hazelcast.getMap("testMapClear");
        String value = map.put("Hello", "World");
        assertEquals(null, value);
        map.clear();
        assertEquals(0,map.size());

        value = map.put("Hello", "World");
        assertEquals(null, value);
        assertEquals("World", map.get("Hello"));
        assertEquals(1,map.size());

        map.remove("Hello");
        assertEquals(0,map.size());
    }

    @Test
    public void testMapRemove(){
        IMap<String,String> map = Hazelcast.getMap("testMapRemove");
        map.put("Hello", "World");
        map.remove("Hello");
        assertEquals(0,map.size());
    }

    @Test
    public void testListAdd(){
        IList<String> list = Hazelcast.getList("testListAdd");
        list.add("Hello World");
        assertEquals(1,list.size());
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListGet(){
        IList<String> list = Hazelcast.getList("testListGet");
        list.add("Hello World");
        assertEquals("Hello World", list.get(0));
    }

    @Test
    public void testListIterator(){
        IList<String> list = Hazelcast.getList("testListIterator");
        list.add("Hello World");
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListListIterator(){
        IList<String> list = Hazelcast.getList("testListListIterator");
        list.add("Hello World");
        assertEquals("Hello World", list.listIterator().next());
    }

    @Test
    public void testSetAdd(){
        ISet<String> set = Hazelcast.getSet("testSetAdd");
        boolean added = set.add("HelloWorld");
        assertEquals(true, added);
        added = set.add("HelloWorld");
        assertFalse(added);
        assertEquals(1,set.size());
    }

    @Test
    public void testSetIterator(){
        ISet<String> set = Hazelcast.getSet("testSetIterator");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        assertEquals("HelloWorld",set.iterator().next());
    }

    @Test
    public void testSetContains(){
        ISet<String> set = Hazelcast.getSet("testSetContains");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        boolean contains = set.contains("HelloWorld");
        assertTrue(contains);
    }

    @Test
    public void testSetClear(){
        ISet<String> set = Hazelcast.getSet("testSetClear");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testSetRemove(){
        ISet<String> set = Hazelcast.getSet("testSetRemove");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.remove("HelloWorld");
        assertEquals(0, set.size());
    }

    @Test
    public void testSetGetName(){
        ISet<String> set = Hazelcast.getSet("testSetGetName");
        assertEquals("testSetGetName", set.getName());
    }

    @Test
    public void testSetAddAll(){
        ISet<String> set = Hazelcast.getSet("testSetAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        set.addAll(Arrays.asList(items));
        assertEquals(4, set.size());

        items = new String[]{"four", "five"};
        set.addAll(Arrays.asList(items));
        assertEquals(5, set.size());
    }

    @Test
    public void testTopicGetName(){
        ITopic<String> topic = Hazelcast.getTopic("testTopicGetName");
        assertEquals("testTopicGetName",topic.getName());
    }

    @Test
    public void testTopicPublish(){
        ITopic<String> topic = Hazelcast.getTopic("testTopicPublish");
        topic.addMessageListener(new MessageListener<String>(){
            public void onMessage(String msg) {
                /*@todo Exceptions on failure are not correctly propagated and test is marked as passing, despite
                * reporting the Exception*/
                assertEquals("Hello World", msg);
            }
        });
        topic.publish("Hello World");
    }

    @Test
    public void testQueueAdd(){
        IQueue<String> queue = Hazelcast.getQueue("testQueueAdd");
        queue.add("Hello World");
        assertEquals(1, queue.size());
    }

    @Test
    public void testQueueAddAll(){
        IQueue<String> queue = Hazelcast.getQueue("testQueueAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertEquals(4, queue.size());
        queue.addAll(Arrays.asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains(){
        IQueue<String> queue = Hazelcast.getQueue("testQueueContains");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertTrue(queue.contains("one"));
        assertTrue(queue.contains("two"));
        assertTrue(queue.contains("three"));
        assertTrue(queue.contains("four"));
    }

    @Test
    public void testQueueContainsAll(){
        IQueue<String> queue = Hazelcast.getQueue("testQueueContainsAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(items);
        queue.addAll(list);
        assertTrue(queue.containsAll(list));
    }

}
