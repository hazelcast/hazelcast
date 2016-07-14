/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapTest extends ReplicatedMapBaseTest {

    @Test
    public void testEmptyMapIsEmpty() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        assertTrue("map should be empty", map.isEmpty());
    }

    @Test
    public void testNonEmptyMapIsNotEmpty() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        map.put(1, 1);
        assertFalse("map should not be empty", map.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTtlThrowsException() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        map.put(1, 1, -1, TimeUnit.DAYS);
    }

    @Test
    public void testAddObject() throws Exception {
        testAdd(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddBinary() throws Exception {
        testAdd(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAdd(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });
    }

    @Test
    public void testPutAllObject() throws Exception {
        testPutAll(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testPutAllBinary() throws Exception {
        testPutAll(buildConfig(InMemoryFormat.BINARY));
    }

    private void testPutAll(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);
        final Map<String, String> mapTest = new HashMap<String, String>();
        for (String key : keys) {
            mapTest.put(key, "bar");
        }

        map1.putAll(mapTest);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });
    }

    @Test
    public void testClearObject() throws Exception {
        testClear(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testClearBinary() throws Exception {
        testClear(buildConfig(InMemoryFormat.BINARY));
    }

    private void testClear(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });

        map1.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, map1.size());
                assertEquals(0, map2.size());
            }
        });
    }

    @Test
    public void testAddTtlObject() throws Exception {
        testAddTtl(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddTtlBinary() throws Exception {
        testAddTtl(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAddTtl(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar", 10, TimeUnit.MINUTES);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    ReplicatedRecord<String, String> record = getReplicatedRecord(map1, key);
                    assertNotNull(record);
                    assertNotEquals(0, record.getTtlMillis());
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map2.get(key));
                    ReplicatedRecord<String, String> record = getReplicatedRecord(map2, key);
                    assertNotNull(record);
                    assertNotEquals(0, record.getTtlMillis());
                }
            }
        });
    }

    @Test
    public void testUpdateObject() throws Exception {
        testUpdate(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testUpdateBinary() throws Exception {
        testUpdate(buildConfig(InMemoryFormat.BINARY));
    }

    private void testUpdate(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });

        for (String key : keys) {
            map2.put(key, "bar2");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar2", map1.get(key));
                    assertEquals("bar2", map2.get(key));
                }
            }
        });
    }

    @Test
    public void testUpdateTtlObject() throws Exception {
        testUpdateTtl(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testUpdateTtlBinary() throws Exception {
        testUpdateTtl(buildConfig(InMemoryFormat.BINARY));
    }

    private void testUpdateTtl(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });

        for (String key : keys) {
            map2.put(key, "bar2", 10, TimeUnit.MINUTES);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar2", map1.get(key));
                    ReplicatedRecord<String, String> record = getReplicatedRecord(map1, key);
                    assertNotNull(record);
                    assertTrue(record.getTtlMillis() > 0);
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar2", map2.get(key));
                    ReplicatedRecord<String, String> record = getReplicatedRecord(map2, key);
                    assertNotNull(record);
                    assertTrue(record.getTtlMillis() > 0);
                }
            }
        });
    }

    @Test
    public void testRemoveObject() throws Exception {
        testRemove(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testRemoveBinary() throws Exception {
        testRemove(buildConfig(InMemoryFormat.BINARY));
    }

    @Test
    public void testContainsKey_returnsFalse_onRemovedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.remove(1);

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testContainsKey_returnsFalse_onNonexistentKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testContainsKey_returnsTrue_onExistingKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);

        assertTrue(map.containsKey(1));
    }

    @Test
    public void testKeySet_notIncludes_removedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        final ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.put(2, Integer.MIN_VALUE);

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Set<Integer> keys = new HashSet<Integer>(map.keySet());
                assertFalse(keys.contains(1));
            }
        }, 20);
    }

    @Test
    public void testEntrySet_notIncludes_removedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        final ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.put(2, Integer.MIN_VALUE);

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Set<Entry<Integer, Integer>> entries = map.entrySet();
                for (Entry<Integer, Integer> entry : entries) {
                    if (entry.getKey().equals(1)) {
                        fail(String.format("We do not expect an entry which's key equals to %d in entry set", 1));
                    }
                }
            }
        }, 20);
    }

    private void testRemove(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertEquals("bar", map1.get(key));
                    assertEquals("bar", map2.get(key));
                }
            }
        });

        for (String key : keys) {
            map2.remove(key);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertFalse(map1.containsKey(key));
                    assertFalse(map2.containsKey(key));
                }
            }
        });
    }

    @Test
    public void testSizeObject() throws Exception {
        testSize(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testSizeBinary() throws Exception {
        testSize(buildConfig(InMemoryFormat.BINARY));
    }


    private void testSize(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);
        final SimpleEntry<String, String>[] testValues = buildTestValues(keys);

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            final ReplicatedMap<String, String> map = i < half ? map1 : map2;
            final SimpleEntry<String, String> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(keys.size(), map1.size());
                assertEquals(keys.size(), map2.size());
            }
        });
    }

    @Test
    public void testContainsKeyObject() throws Exception {
        testContainsKey(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testContainsKeyBinary() throws Exception {
        testContainsKey(buildConfig(InMemoryFormat.BINARY));
    }

    private void testContainsKey(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertTrue(map1.containsKey(key));
                    assertTrue(map2.containsKey(key));
                }
            }
        });
    }

    @Test
    public void testContainsValue_returnsFalse_onNonexistentValue() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        assertFalse(map.containsValue(1));
    }

    @Test
    public void testContainsValueObject() throws Exception {
        testContainsValue(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testContainsValueBinary() throws Exception {
        testContainsValue(buildConfig(InMemoryFormat.BINARY));
    }

    private void testContainsValue(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        int half = keys.size() / 2, i = 0;
        for (String key : keys) {
            final ReplicatedMap<String, String> map = i++ < half ? map1 : map2;
            map.put(key, key);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (String key : keys) {
                    assertTrue(map1.containsValue(key));
                    assertTrue(map2.containsValue(key));
                }
            }
        });
    }

    @Test
    public void testValuesWithComparator() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        Collection<Integer> values = map.values(new DescendingComparator());
        int v = 100;
        for (Integer value : values) {
            assertEquals(--v, (int) value);
        }
    }

    @Test
    public void testValuesObject() throws Exception {
        testValues(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testValuesBinary() throws Exception {
        testValues(buildConfig(InMemoryFormat.BINARY));
    }

    private void testValues(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        int half = keys.size() / 2, i = 0;
        for (String key : keys) {
            final ReplicatedMap<String, String> map = i++ < half ? map1 : map2;
            map.put(key, key);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(keys, new HashSet<String>(map1.values()));
                assertEquals(keys, new HashSet<String>(map2.values()));
            }
        });
    }

    @Test
    public void testKeySetObject() throws Exception {
        testKeySet(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testKeySetBinary() throws Exception {
        testKeySet(buildConfig(InMemoryFormat.BINARY));
    }

    private void testKeySet(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        int half = keys.size() / 2, i = 0;
        for (String key : keys) {
            final ReplicatedMap<String, String> map = i++ < half ? map1 : map2;
            map.put(key, key);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(keys, new HashSet<String>(map1.keySet()));
                assertEquals(keys, new HashSet<String>(map2.keySet()));
            }
        });
    }

    @Test
    public void testEntrySetObject() throws Exception {
        testEntrySet(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testEntrySetBinary() throws Exception {
        testEntrySet(buildConfig(InMemoryFormat.BINARY));
    }

    private void testEntrySet(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        int half = keys.size() / 2, i = 0;
        for (String key : keys) {
            final ReplicatedMap<String, String> map = i++ < half ? map1 : map2;
            map.put(key, key);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<Entry<String, String>> entrySet1 = new ArrayList<Entry<String, String>>(map1.entrySet());
                List<Entry<String, String>> entrySet2 = new ArrayList<Entry<String, String>>(map2.entrySet());
                assertEquals(keys.size(), entrySet1.size());
                assertEquals(keys.size(), entrySet2.size());

                for (Entry<String, String> e : entrySet1) {
                    assertTrue(keys.contains(e.getKey()));
                }

                for (Entry<String, String> e : entrySet2) {
                    assertTrue(keys.contains(e.getKey()));
                }
            }
        });
    }

    @Test
    public void testAddListenerObject() throws Exception {
        testAddEntryListener(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddListenerBinary() throws Exception {
        testAddEntryListener(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAddEntryListener(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        SimpleEntryListener listener = new SimpleEntryListener(1, 0);
        map2.addEntryListener(listener, keys.iterator().next());

        for (String key : keys) {
            map1.put(key, "bar");
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testEvictionObject() throws Exception {
        testEviction(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testEvictionBinary() throws Exception {
        testEviction(buildConfig(InMemoryFormat.BINARY));
    }

    private void testEviction(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int partitionCount = getPartitionService(instance1).getPartitionCount();
        final Set<String> keys = generateRandomKeys(instance1, partitionCount);

        SimpleEntryListener listener = new SimpleEntryListener(0, 100);
        map2.addEntryListener(listener);

        SimpleEntryListener listenerKey = new SimpleEntryListener(0, 1);
        map1.addEntryListener(listenerKey, keys.iterator().next());

        for (String key : keys) {
            map1.put(key, "bar", 3, TimeUnit.SECONDS);
        }

        assertOpenEventually(listener.evictLatch);
        assertOpenEventually(listenerKey.evictLatch);
    }

    private class SimpleEntryListener extends EntryAdapter<String, String> {
        CountDownLatch addLatch;
        CountDownLatch evictLatch;

        SimpleEntryListener(int addCount, int evictCount) {
            addLatch = new CountDownLatch(addCount);
            evictLatch = new CountDownLatch(evictCount);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            addLatch.countDown();
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            evictLatch.countDown();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.put(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.remove(null);
    }

    @Test
    public void removeEmptyListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        assertFalse(map1.removeEntryListener("2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNullListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.removeEntryListener(null);
    }

    @Test
    public void testSizeAfterRemove() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.remove(1);
        assertTrue(map.size() == 0);
    }

    @Test
    public void testDestroy() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        ReplicatedMap<Object, Object> replicatedMap = instance.getReplicatedMap(randomName());
        replicatedMap.put(1, 1);
        replicatedMap.destroy();
        Collection<DistributedObject> objects = instance.getDistributedObjects();
        assertEquals(0, objects.size());
    }

    class DescendingComparator implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 == o2 ? 0 : o1 > o2 ? -1 : 1;
        }
    }

}
