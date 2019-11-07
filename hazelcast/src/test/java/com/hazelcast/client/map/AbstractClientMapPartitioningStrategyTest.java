/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientMapConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractClientMapPartitioningStrategyTest extends HazelcastTestSupport {

    private static TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member;
    private HazelcastInstance client;
    protected String mapName;
    private IMap<String, String> memberMap;
    private IMap<String, String> clientMap;
    private IMap<String, String> clientMapWithoutStrategy;
    private String key;

    public abstract ClientConfig getClientConfig();

    @Before
    public void setup() {
        PartitioningStrategyConfig partitioningStrategyConfig
                = new PartitioningStrategyConfig(StringPartitioningStrategy.class.getName());
        mapName = randomMapName();
        Config config = new Config();
        config.addMapConfig(new MapConfig(mapName)
                .setPartitioningStrategyConfig(partitioningStrategyConfig));
        member = factory.newHazelcastInstance(config);
        memberMap = member.getMap(mapName);

        client = factory.newHazelcastClient(getClientConfig());
        clientMap = client.getMap(mapName);

        HazelcastInstance clientWithoutStrategy = factory.newHazelcastClient();
        clientMapWithoutStrategy = clientWithoutStrategy.getMap(mapName);

        memberMap.clear();
        key = generateKey();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testEntriesWithSamePartitionKeyGoToSamePartitionWithStrategy() {
        BiTuple<String, String> keys = generateKeysOnDifferentPartitions();
        assertNull(clientMap.put(keys.element1(), "value1"));
        assertNull(clientMap.put(keys.element2(), "value2"));

        RecordStore recordStore = getRecordStore(getPartitionId(StringPartitioningStrategy.getPartitionKey(keys.element1())));
        assertTrue(recordStore.existInMemory(toDataWithStrategy(keys.element1())));
        assertTrue(recordStore.existInMemory(toDataWithStrategy(keys.element2())));
    }

    @Test
    public void testContainsKey() {
        memberMap.put(key, "value");
        assertTrue(clientMap.containsKey(key));
        assertFalse(clientMapWithoutStrategy.containsKey(key));
    }

    @Test
    public void testGet() {
        memberMap.put(key, "value");
        assertEquals("value", clientMap.get(key));
        assertNull(clientMapWithoutStrategy.get(key));
    }

    @Test
    public void testPut() {
        assertNull(clientMapWithoutStrategy.put(key, "value"));
        assertNull(memberMap.get(key));
        assertNull(clientMap.put(key, "value"));
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testRemove() {
        memberMap.put(key, "value");
        assertNull(clientMapWithoutStrategy.remove(key));
        assertEquals("value", clientMap.remove(key));
    }

    @Test
    public void testRemoveWithValue() {
        memberMap.put(key, "value");
        assertFalse(clientMapWithoutStrategy.remove(key, "value"));
        assertTrue(clientMap.remove(key, "value"));
    }

    @Test
    public void testRemoveAllWithPartitionPredicate() {
        memberMap.put(key, "value");
        PartitionPredicate<String, String> predicate = Predicates.partitionPredicate(key, Predicates.equal("this", "value"));
        clientMapWithoutStrategy.removeAll(predicate);
        assertEquals(1, clientMapWithoutStrategy.size());
        clientMap.removeAll(predicate);
        assertEquals(0, clientMap.size());
    }

    @Test
    public void testDelete() {
        memberMap.put(key, "value");
        clientMapWithoutStrategy.delete(key);
        assertEquals(1, clientMapWithoutStrategy.size());
        clientMap.delete(key);
        assertEquals(0, clientMap.size());
    }

    @Test
    public void testGetAsync() throws ExecutionException, InterruptedException {
        memberMap.put(key, "value");
        assertNull(clientMapWithoutStrategy.getAsync(key).toCompletableFuture().get());
        assertEquals("value", clientMap.getAsync(key).toCompletableFuture().get());
    }

    @Test
    public void testPutAsync() throws ExecutionException, InterruptedException {
        assertNull(clientMapWithoutStrategy.putAsync(key, "value").toCompletableFuture().get());
        assertNull(memberMap.get(key));
        assertNull(clientMap.putAsync(key, "value").toCompletableFuture().get());
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testSetAsync() throws ExecutionException, InterruptedException {
        clientMapWithoutStrategy.setAsync(key, "value").toCompletableFuture().get();
        assertNull(memberMap.get(key));
        clientMap.setAsync(key, "value").toCompletableFuture().get();
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testRemoveAsync() throws ExecutionException, InterruptedException {
        memberMap.put(key, "value");
        assertNull(clientMapWithoutStrategy.removeAsync(key).toCompletableFuture().get());
        assertEquals("value", clientMap.removeAsync(key).toCompletableFuture().get());
    }

    @Test
    public void testTryRemove() {
        memberMap.put(key, "value");
        assertFalse(clientMapWithoutStrategy.tryRemove(key, 100, TimeUnit.MILLISECONDS));
        assertTrue(clientMap.tryRemove(key, 100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryPut() {
        assertTrue(clientMapWithoutStrategy.tryPut(key, "value", 100, TimeUnit.MILLISECONDS));
        assertNull(memberMap.get(key));
        assertTrue(clientMap.tryPut(key, "value", 100, TimeUnit.MILLISECONDS));
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testPutTransient() {
        clientMapWithoutStrategy.putTransient(key, "value", 0, TimeUnit.MILLISECONDS);
        assertNull(memberMap.get(key));
        clientMap.putTransient(key, "value", 0, TimeUnit.MILLISECONDS);
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testPutIfAbsent() {
        assertNull(clientMapWithoutStrategy.putIfAbsent(key, "value"));
        assertNull(memberMap.get(key));
        assertNull(clientMap.putIfAbsent(key, "value"));
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testReplaceIfSame() {
        memberMap.put(key, "value");
        assertFalse(clientMapWithoutStrategy.replace(key, "value", "value2"));
        assertEquals("value", memberMap.get(key));
        assertTrue(clientMap.replace(key, "value", "value2"));
        assertEquals("value2", memberMap.get(key));
    }

    @Test
    public void testReplace() {
        memberMap.put(key, "value");
        assertNull(clientMapWithoutStrategy.replace(key, "value2"));
        assertEquals("value", memberMap.get(key));
        assertEquals("value", clientMap.replace(key, "value2"));
        assertEquals("value2", memberMap.get(key));
    }

    @Test
    public void testSet() {
        clientMapWithoutStrategy.set(key, "value");
        assertNull(memberMap.get(key));
        clientMap.set(key, "value");
        assertEquals("value", memberMap.get(key));
    }

    @Test
    public void testLock() {
        try {
            clientMapWithoutStrategy.lock(key);
            assertTrue(clientMapWithoutStrategy.isLocked(key));
            assertFalse(clientMap.isLocked(key));
            clientMap.lock(key);
            assertTrue(clientMap.isLocked(key));
        } finally {
            clientMapWithoutStrategy.forceUnlock(key);
            clientMap.forceUnlock(key);
        }
    }

    @Test
    public void testTryLock() {
        try {
            clientMapWithoutStrategy.tryLock(key);
            assertTrue(clientMapWithoutStrategy.isLocked(key));
            assertFalse(clientMap.isLocked(key));
            clientMap.tryLock(key);
            assertTrue(clientMap.isLocked(key));
        } finally {
            clientMapWithoutStrategy.forceUnlock(key);
            clientMap.forceUnlock(key);
        }
    }

    @Test
    public void testUnlock() {
        try {
            clientMapWithoutStrategy.lock(key);
            assertTrue(clientMapWithoutStrategy.isLocked(key));
            clientMapWithoutStrategy.unlock(key);
            assertFalse(clientMapWithoutStrategy.isLocked(key));

            clientMap.lock(key);
            assertTrue(clientMap.isLocked(key));
            clientMap.unlock(key);
            assertFalse(clientMap.isLocked(key));
        } finally {
            clientMapWithoutStrategy.forceUnlock(key);
            clientMap.forceUnlock(key);
        }
    }

    @Test
    public void testGetEntryView() {
        memberMap.put(key, "value");
        assertEquals("value", clientMap.getEntryView(key).getValue());
        assertNull(clientMapWithoutStrategy.getEntryView(key));
    }

    @Test
    public void testEvict() {
        memberMap.put(key, "value");
        assertFalse(clientMapWithoutStrategy.evict(key));
        assertEquals(1, clientMapWithoutStrategy.size());
        assertTrue(clientMap.evict(key));
        assertEquals(0, clientMap.size());
    }

    @Test
    public void testGetAll() {
        memberMap.put(key, "value");
        Set<String> keys = new HashSet<>();
        keys.add(key);
        Map<String, String> map = new HashMap<>();
        map.put(key, "value");
        assertEquals(map, clientMap.getAll(keys));
        assertEquals(Collections.emptyMap(), clientMapWithoutStrategy.getAll(keys));
    }

    @Test
    public void testSetTtl() {
        memberMap.put(key, "value");
        assertFalse(clientMapWithoutStrategy.setTtl(key, 100, TimeUnit.MILLISECONDS));
        assertTrue(clientMap.setTtl(key, 100, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testExecuteOnKey() {
        memberMap.put(key, "value");
        ValueUpdater updater = new ValueUpdater("value2");
        assertNull(clientMapWithoutStrategy.executeOnKey(key, updater));
        assertNull(clientMapWithoutStrategy.get(key));
        assertEquals("value2", clientMap.executeOnKey(key, updater));
        assertEquals("value2", clientMap.get(key));
    }

    @Test
    public void testSubmitToKeyWithCallback() {
        memberMap.put(key, "value");
        AtomicReference<String> response1 = new AtomicReference<>();
        AtomicReference<String> response2 = new AtomicReference<>();
        ValueUpdater updater = new ValueUpdater("value2");
        clientMapWithoutStrategy.submitToKey(key, updater, new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                response1.set(response);
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.getMessage());
            }
        });
        clientMap.submitToKey(key, updater, new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                response2.set(response);
            }

            @Override
            public void onFailure(Throwable t) {
                fail(t.getMessage());
            }
        });

        assertTrueEventually(() -> {
            assertNull(response1.get());
        });
        assertTrueEventually(() -> {
            assertEquals("value2", response2.get());
        });
    }

    @Test
    public void testSubmitToKey() throws ExecutionException, InterruptedException {
        memberMap.put(key, "value");
        ValueUpdater updater = new ValueUpdater("value2");
        assertNull(clientMapWithoutStrategy.submitToKey(key, updater).toCompletableFuture().get());
        assertEquals("value2", clientMap.submitToKey(key, updater).toCompletableFuture().get());
    }

    @Test
    public void testExecuteOnKeys() {
        memberMap.put(key, "value");
        Set<String> keys = new HashSet<>();
        keys.add(key);
        Map<String, String> map = new HashMap<>();
        map.put(key, "value2");
        ValueUpdater updater = new ValueUpdater("value2");
        assertEquals(Collections.emptyMap(), clientMapWithoutStrategy.executeOnKeys(keys, updater));
        assertEquals(map, clientMap.executeOnKeys(keys, updater));
    }

    @Test
    public void testSubmitToKeys() throws ExecutionException, InterruptedException {
        memberMap.put(key, "value");
        Set<String> keys = new HashSet<>();
        keys.add(key);
        Map<String, String> map = new HashMap<>();
        map.put(key, "value2");
        ValueUpdater updater = new ValueUpdater("value2");
        assertEquals(Collections.emptyMap(), ((ClientMapProxy<String, String>)clientMapWithoutStrategy).submitToKeys(keys, updater).toCompletableFuture().get());
        assertEquals(map, ((ClientMapProxy<String, String>)clientMap).submitToKeys(keys, updater).toCompletableFuture().get());
    }

    @Test
    public void testPutAll() {
        Map<String, String> map = new HashMap<>();
        map.put(key, "value");
        clientMapWithoutStrategy.putAll(map);
        assertNull(memberMap.get(key));
        clientMap.putAll(map);
        assertEquals("value", memberMap.get(key));
    }

    public static class ValueUpdater implements EntryProcessor<String, String, String> {

        private final String newValue;

        ValueUpdater(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public String process(Map.Entry<String, String> entry) {
            if (entry.getValue() != null) {
                entry.setValue(newValue);
                return newValue;
            }
            return null;
        }
    }

    // Returns a random key that would go to different partitions
    // depending on whether the StringPartitioningStrategy set or not.
    private String generateKey() {
        String key;
        int partitionIdOfFullKey, partitionIdOfPartitionKey;
        do {
            key = generateRandomString(5) + "@" + generateRandomString(5);
            partitionIdOfFullKey = getPartitionId(key);
            partitionIdOfPartitionKey = getPartitionId(StringPartitioningStrategy.getPartitionKey(key));
        } while (partitionIdOfFullKey == partitionIdOfPartitionKey);
        return key;
    }

    // Returns a tuple of random keys that would go to different partitions
    // when StringPartitioningStrategy is not used.
    private BiTuple<String, String> generateKeysOnDifferentPartitions() {
        String partitionKey = generateRandomString(5);
        String key1 = generateRandomString(5) + "@" + partitionKey;
        String key2;
        do {
            key2 = generateRandomString(5) + "@" + partitionKey;
        } while (getPartitionId(key1) == getPartitionId(key2));
        return BiTuple.of(key1, key2);
    }

    private int getPartitionId(Object key) {
        return client.getPartitionService().getPartition(key).getPartitionId();
    }

    private RecordStore getRecordStore(int partitionId) {
        MapService mapService = getNodeEngineImpl(member).getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext()
                .getPartitionContainer(partitionId)
                .getExistingRecordStore(mapName);
    }

    private Data toDataWithStrategy(Object key) {
        return ((HazelcastClientProxy) client).getSerializationService()
                .toData(key, StringPartitioningStrategy.INSTANCE);
    }
}
