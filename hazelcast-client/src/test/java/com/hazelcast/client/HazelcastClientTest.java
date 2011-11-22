/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.*;
import com.hazelcast.core.InstanceEvent.InstanceEventType;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

public class HazelcastClientTest extends HazelcastClientTestBase {

    @BeforeClass
    public static void before() {
        single.destroy();
        single.init();
    }

    @AfterClass
    public static void after() {
        System.out.println("AfterClass " + runningTestName);
        single.destroy();
    }

    @Test
    public void testGetClusterMemberSize() {
        Cluster cluster = getHazelcastClient().getCluster();
        Set<com.hazelcast.core.Member> members = cluster.getMembers();
        //Tests are run with only one member in the cluster, this may change later
        assertEquals(1, members.size());
    }

    @Test
    public void iterateOverMembers() {
        Cluster cluster = getHazelcastClient().getCluster();
        Set<com.hazelcast.core.Member> members = cluster.getMembers();
        for (Member member : members) {
            assertNotNull(member);
        }
    }

    @Test
    public void addInstanceListener() throws InterruptedException {
        final CountDownLatch destroyedLatch = new CountDownLatch(1);
        final CountDownLatch createdLatch = new CountDownLatch(1);
        final IMap<Integer, Integer> instance = getHazelcastClient().getMap("addInstanceListener");
        InstanceListener listener = new InstanceListener() {

            public void instanceDestroyed(InstanceEvent event) {
                assertEquals(InstanceEventType.DESTROYED, event.getEventType());
                assertEquals(instance, event.getInstance());
                destroyedLatch.countDown();
            }

            public void instanceCreated(InstanceEvent event) {
                assertEquals(InstanceEventType.CREATED, event.getEventType());
                IMap<Integer, Integer> map = (IMap<Integer, Integer>) event.getInstance();
                assertEquals(instance.getName(), map.getName());
                createdLatch.countDown();
            }
        };
        getHazelcastClient().addInstanceListener(listener);
        instance.put(1, 1);
        assertEquals(1, instance.size());
        assertTrue(createdLatch.await(10, TimeUnit.SECONDS));
        instance.destroy();
        assertTrue(destroyedLatch.await(10, TimeUnit.SECONDS));
        getHazelcastClient().removeInstanceListener(listener);
    }

    @Test
    public void testGetClusterTime() {
        Cluster cluster = getHazelcastClient().getCluster();
        long clusterTime = cluster.getClusterTime();
        assertTrue(clusterTime > 0);
    }

    @Ignore
    public void testProxySerialization() {
        IMap<Object, Object> mapProxy = getHazelcastClient().getMap("proxySerialization");
        ILock mapLock = getHazelcastClient().getLock(mapProxy);
        assertNotNull(mapLock);
    }

    @Test
    public void testMapGetName() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapGetName");
        assertEquals("testMapGetName", map.getName());
    }

    @Test
    public void testMapValuesSize() {
        Map<String, String> map = getHazelcastClient().getMap("testMapValuesSize");
        map.put("Hello", "World");
        assertEquals(1, map.values().size());
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
        value = map.put("Hello", "New World");
        assertEquals("New World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
    }

    @Test
    public void testPutDate() {
        Map<String, Date> map = getHazelcastClient().getMap("putDate");
        Date date = new Date();
        map.put("key", date);
        Date d = map.get("key");
        assertEquals(date.getTime(), d.getTime());
    }

    @Test
    public void testPutBigInteger() {
        Map<String, BigInteger> map = getHazelcastClient().getMap("putBigInteger");
        BigInteger number = new BigInteger("12312312312");
        map.put("key", number);
        BigInteger b = map.get("key");
        assertEquals(number, b);
    }

    @Test
    public void testMapReplaceIfSame() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapReplaceIfSame");
        assertFalse(map.replace("Hello", "Java", "World"));
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        assertFalse(map.replace("Hello", "Java", "NewWorld"));
        assertTrue(map.replace("Hello", "World", "NewWorld"));
        assertEquals("NewWorld", map.get("Hello"));
        assertEquals(1, map.size());
    }

    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMapContainsValue() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMapClear() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapClear");
        String value = map.put("Hello", "World");
        assertEquals(null, value);
        map.clear();
        assertEquals(0, map.size());
        value = map.put("Hello", "World");
        assertEquals(null, value);
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        map.remove("Hello");
        assertEquals(0, map.size());
    }

    @Test
    public void testMapRemove() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapRemove");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
        map.remove("Hello");
        assertEquals(0, map.size());
        assertEquals(0, map.keySet().size());
        map.put("Hello", "World");
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
    }

    @Test
    public void testMapPutAll() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapPutAll");
        Map<String, String> m = new HashMap<String, String>();
        m.put("Hello", "World");
        m.put("hazel", "cast");
        map.putAll(m);
        assertEquals(2, map.size());
        assertTrue(map.containsKey("Hello"));
        assertTrue(map.containsKey("hazel"));
    }

    @Test
    public void testMapEntrySet() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapEntrySet");
        map.put("Hello", "World");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            assertEquals("Hello", e.getKey());
            assertEquals("World", e.getValue());
        }
    }

    @Test
    public void testMapEntrySetWhenRemoved() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapEntrySetWhenRemoved");
        map.put("Hello", "World");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        map.remove("Hello");
        for (IMap.Entry<String, String> e : set) {
            assertTrue(e.getValue().equals("World"));
        }
    }

    @Test
    public void testMapEntryListener() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapEntrySet");
        final CountDownLatch latchAdded = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent<String, String> event) {
                assertEquals("world", event.getValue());
                assertEquals("hello", event.getKey());
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent<String, String> event) {
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent<String, String> event) {
                assertEquals("world", event.getOldValue());
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
                latchUpdated.countDown();
            }

            public void entryEvicted(EntryEvent<String, String> event) {
                entryRemoved(event);
            }
        }, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchUpdated.await(10, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }
    
    @Test
    /**
     * Test for Issue 686
     */
    public void testMapEntryListenerThrowsException() throws InterruptedException {
    	final String mapName = "testMapListenerThrowException";
    	IMap c = getHazelcastClient().getMap(mapName);
    	IMap m = getHazelcastInstance().getMap(mapName);
    	
    	m.addEntryListener(new EntryListener() {
			public void entryAdded(EntryEvent event) {
				throw new RuntimeException("dummy exception");
			}
			public void entryRemoved(EntryEvent event) {
			}
			public void entryUpdated(EntryEvent event) {
			}
			public void entryEvicted(EntryEvent event) {
			}
		}, false);
    	
    	final int k = 5;
    	final CountDownLatch latch = new CountDownLatch(k);
    	c.addEntryListener(new EntryListener() {
			public void entryAdded(EntryEvent event) {
				System.out.println(event);
				latch.countDown();
			}
			public void entryRemoved(EntryEvent event) {
			}
			public void entryUpdated(EntryEvent event) {
			}
			public void entryEvicted(EntryEvent event) {
			}
		}, false);
    	
    	for (int i = 0; i < k; i++) {
			if(i % 2 == 0) {
				m.put(i, i);
			} else {
				c.put(i, i);
			}
		}
    	assertTrue("Error on client EntryListener!", latch.await(10, TimeUnit.SECONDS));
    	single.shutdownHazelcastClient();
    }

    @Test
    public void testMapEvict() {
        IMap<String, String> map = getHazelcastClient().getMap("testMapEviction");
        map.put("currentIteratedKey", "currentIteratedValue");
        assertEquals(true, map.containsKey("currentIteratedKey"));
        map.evict("currentIteratedKey");
        assertEquals(false, map.containsKey("currentIteratedKey"));
    }

    @Test
    public void testListAdd() {
        IList<String> list = getHazelcastClient().getList("testListAdd");
        list.add("Hello World");
        assertEquals(1, list.size());
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListContains() {
        IList<String> list = getHazelcastClient().getList("testListContains");
        list.add("Hello World");
        assertTrue(list.contains("Hello World"));
    }

    @Test
    public void testListGet() {
        // Unsupported
        //IList<String> list = getHazelcastClient().getList("testListGet");
        //list.add("Hello World");
        //assertEquals("Hello World", list.get(0));
    }

    @Test
    public void testListIterator() {
        IList<String> list = getHazelcastClient().getList("testListIterator");
        list.add("Hello World");
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListListIterator() {
        // Unsupported
        //IList<String> list = getHazelcastClient().getList("testListListIterator");
        //list.add("Hello World");
        //assertEquals("Hello World", list.listIterator().next());
    }

    @Test
    public void testListIndexOf() {
        // Unsupported
        //IList<String> list = getHazelcastClient().getList("testListIndexOf");
        //list.add("Hello World");
        //assertEquals(0, list.indexOf("Hello World"));
    }

    @Test
    public void testListIsEmpty() {
        IList<String> list = getHazelcastClient().getList("testListIsEmpty");
        assertTrue(list.isEmpty());
        list.add("Hello World");
        assertFalse(list.isEmpty());
    }

    @Test
    @Ignore
    public void testListItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        IList<String> list = getHazelcastClient().getList("testListListener");
        list.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        list.add("hello");
        list.remove("hello");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    @Ignore
    public void testSetItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        ISet<String> set = getHazelcastClient().getSet("testSetListener");
        set.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        set.add("hello");
        set.remove("hello");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testQueueItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        IQueue<String> queue = getHazelcastClient().getQueue("testQueueListener");
        queue.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        queue.offer("hello");
        assertEquals("hello", queue.poll());
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testSetAdd() {
        ISet<String> set = getHazelcastClient().getSet("testSetAdd");
        boolean added = set.add("HelloWorld");
        assertEquals(true, added);
        added = set.add("HelloWorld");
        assertFalse(added);
        assertEquals(1, set.size());
    }

    @Test
    public void testSetIterator() {
        ISet<String> set = getHazelcastClient().getSet("testSetIterator");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        assertEquals("HelloWorld", set.iterator().next());
    }

    @Test
    public void testSetContains() {
        ISet<String> set = getHazelcastClient().getSet("testSetContains");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        boolean contains = set.contains("HelloWorld");
        assertTrue(contains);
    }

    @Test
    public void testSetClear() {
        ISet<String> set = getHazelcastClient().getSet("testSetClear");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testSetRemove() {
        ISet<String> set = getHazelcastClient().getSet("testSetRemove");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.remove("HelloWorld");
        assertEquals(0, set.size());
        assertTrue(set.add("HelloWorld"));
        assertFalse(set.add("HelloWorld"));
        assertEquals(1, set.size());
    }

    @Test
    public void testSetGetName() {
        ISet<String> set = getHazelcastClient().getSet("testSetGetName");
        assertEquals("testSetGetName", set.getName());
    }

    @Test
    public void testSetAddAll() {
        ISet<String> set = getHazelcastClient().getSet("testSetAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        set.addAll(Arrays.asList(items));
        assertEquals(4, set.size());
        items = new String[]{"four", "five"};
        set.addAll(Arrays.asList(items));
        assertEquals(5, set.size());
    }

    @Test
    public void testTopicGetName() {
        ITopic<String> topic = getHazelcastClient().getTopic("testTopicGetName");
        assertEquals("testTopicGetName", topic.getName());
    }

    @Test
    public void testTopicPublish() {
        ITopic<String> topic = getHazelcastClient().getTopic("testTopicPublish");
        final CountDownLatch latch = new CountDownLatch(1);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                assertEquals("Hello World", msg);
                latch.countDown();
            }
        });
        topic.publish("Hello World");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testQueueAdd() {
        IQueue<String> queue = getHazelcastClient().getQueue("testQueueAdd");
        queue.add("Hello World");
        assertEquals(1, queue.size());
    }

    @Test
    public void testQueueAddAll() {
        IQueue<String> queue = getHazelcastClient().getQueue("testQueueAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertEquals(4, queue.size());
        queue.addAll(Arrays.asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        IQueue<String> queue = getHazelcastClient().getQueue("testQueueContains");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertTrue(queue.contains("one"));
        assertTrue(queue.contains("two"));
        assertTrue(queue.contains("three"));
        assertTrue(queue.contains("four"));
    }

    @Test
    public void testQueueContainsAll() {
        IQueue<String> queue = getHazelcastClient().getQueue("testQueueContainsAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(items);
        queue.addAll(list);
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void testIdGenerator() {
        IdGenerator id = getHazelcastClient().getIdGenerator("testIdGenerator");
        assertEquals(1, id.newId());
        assertEquals(2, id.newId());
        assertEquals("testIdGenerator", id.getName());
    }

    @Test
    public void testLock() {
        ILock lock = getHazelcastClient().getLock("testLock");
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @Test
    public void testGetMapEntryHits() {
        IMap<String, String> map = getHazelcastClient().getMap("testGetMapEntryHits");
        map.put("Hello", "World");
        MapEntry<String, String> me = map.getMapEntry("Hello");
        assertEquals(0, me.getHits());
        map.get("Hello");
        map.get("Hello");
        map.get("Hello");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getHits());
    }

    @Test
    public void testGetMapEntryVersion() {
        IMap<String, String> map = getHazelcastClient().getMap("testGetMapEntryVersion");
        map.put("Hello", "World");
        MapEntry<String, String> me = map.getMapEntry("Hello");
        assertEquals(0, me.getVersion());
        map.put("Hello", "1");
        map.put("Hello", "2");
        map.put("Hello", "3");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getVersion());
    }

    @Test
    @Ignore
    public void testMapInstanceDestroy() throws Exception {
        IMap<String, String> map = getHazelcastClient().getMap("testMapDestroy");
        Thread.sleep(1000);
        Collection<Instance> instances = getHazelcastClient().getInstances();
        boolean found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType().isMap()) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMapDestroy")) {
                    found = true;
                }
            }
        }
        assertTrue(found);
        map.destroy();
        Thread.sleep(1000);
        found = false;
        instances = getHazelcastClient().getInstances();
        for (Instance instance : instances) {
            if (instance.getInstanceType().isMap()) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMapDestroy")) {
                    found = true;
                }
            }
        }
        assertFalse(found);
    }

    @Test
    public void newSerializer() {
        final String str = "Fuad";
        byte[] b = IOUtil.toByte(str);
        assertEquals(str, IOUtil.toObject(b));
    }

    @Test
    public void newSerializerExternalizable() {
        final ExternalizableImpl o = new ExternalizableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        byte[] b = IOUtil.toByte(o);
        assertFalse(b.length == 0);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        final ExternalizableImpl object = (ExternalizableImpl) IOUtil.toObject(b);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }

    @Test
    public void testExternalizable() {
        IMap<String, Object> map = getHazelcastClient().getMap("testExternalizable");
        final ExternalizableImpl o = new ExternalizableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        map.put("Hello", o);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        final ExternalizableImpl object = (ExternalizableImpl) single.getHazelcastInstance().getMap("testExternalizable").get("Hello");
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }

    public static class ExternalizableImpl implements Externalizable {
        private int v;
        private String s;

        private boolean readExternal = false;
        private boolean writeExternal = false;

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof ExternalizableImpl)) return false;
            final ExternalizableImpl other = (ExternalizableImpl) obj;
            return this.v == other.v &&
                    ((this.s == null && other.s == null) ||
                            (this.s != null && this.s.equals(other.s)));
        }

        @Override
        public int hashCode() {
            return this.v + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }

    @Test
    public void testGetPartitions() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            getHazelcastClient().getMap("def").put(i, i);
        }
        assertPartitionsUnique();
    }

    @Test
    public void testGetPartition() throws InterruptedException {
        IMap map = getHazelcastClient().getMap("def");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < 1000; i++) {
            Partition p1 = single.getHazelcastInstance().getPartitionService().getPartition(i);
            Partition p2 = single.getHazelcastClient().getPartitionService().getPartition(i);
            assertEquals(p1.getPartitionId(), p2.getPartitionId());
            assertEquals(p1.getOwner(), p2.getOwner());
            set.add(p1.getPartitionId());
        }
        System.out.println("Size: " + set.size());
    }

    @Test
    public void testGetPartitionsFromDifferentThread() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            getHazelcastClient().getMap("def").put(i, i);
        }
        new Thread(new Runnable() {

            public void run() {
                assertPartitionsUnique();
            }
        }).start();
    }

    private void assertPartitionsUnique() {
        Set set = new HashSet();
        PartitionService partitionService = getHazelcastClient().getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        assertEquals(271, partitions.size());
        for (Partition partition : partitions) {
            assertNotNull(partition.getPartitionId());
            assertTrue(set.add(partition.getPartitionId()));
        }
    }
}
