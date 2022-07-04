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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class EntryLoaderSimpleTest extends HazelcastTestSupport {

    @Parameters(name = "inMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    private TestEntryLoader testEntryLoader = new TestEntryLoader();

    protected HazelcastInstance[] instances;

    protected IMap<String, String> map;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void setup() {
        instances = createInstances();
        map = instances[0].getMap(randomMapName());
    }

    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(1).newInstances(getConfig());
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setImplementation(testEntryLoader);
        config.getMapConfig("default")
                .setMapStoreConfig(mapStoreConfig)
                .setInMemoryFormat(inMemoryFormat);
        return config;
    }

    @Test
    public void testEntryWithExpirationTime_expires() {
        testEntryLoader.putExternally("key", "val", System.currentTimeMillis() + 10000);
        assertEquals("val", map.get("key"));
        sleepAtLeastMillis(10000);
        assertNull(map.get("key"));
    }

    @Test
    public void testEntryWithoutExpirationTimeLoads() {
        testEntryLoader.putExternally("key", "val");
        assertEquals("val", map.get("key"));
    }

    @Test
    public void testLoadWithNullDoesNothing() {
        assertNull(map.get(TestEntryLoader.NULL_RETURNING_KEY));
        assertEquals(0, map.size());
    }

    @Test
    public void testLoadAllWithExpirationTimes() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i, System.currentTimeMillis() + 10000);
        }
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, "val" + i);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testLoadAllDoesNotPutEntriesWithPastExpiration() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i, System.currentTimeMillis() - 1000);
        }
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, null);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testLoadAllWithoutExpirationTimes() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 0, entryCount);
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, "val" + i);
        }
        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
    }

    @Test
    public void testGetAllLoadsEntriesWithExpiration() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 5000, 0, entryCount);
        Set<String> requestedKeys = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            requestedKeys.add("key" + i);
        }
        Map<String, String> entries = map.getAll(requestedKeys);
        for (int i = 0; i < 50; i++) {
            assertEquals("val" + i, entries.get("key" + i));
        }
        sleepAtLeastSeconds(6);
        for (int i = 0; i < 50; i++) {
            assertInMemory(instances, map.getName(), "key" + i, null);
        }
        assertExpiredEventually(map, "key", 0, 50);
    }

    @Test
    public void testGetAllLoadsEntriesWithoutExpiration() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 0, entryCount);
        Set<String> requestedKeys = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            requestedKeys.add("key" + i);
        }
        Map<String, String> entries = map.getAll(requestedKeys);
        for (int i = 0; i < 50; i++) {
            assertEquals("val" + i, entries.get("key" + i));
        }
    }

    @Test
    public void testPut_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.put("key", "val2"));
    }

    @Test
    public void testPutWithTtl() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.put("key", "val2", 10, TimeUnit.SECONDS));
    }

    @Test
    public void testPutWithMaxIdle_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.put("key", "val2", 10, TimeUnit.DAYS, 5, TimeUnit.DAYS));
    }

    @Test
    public void testPutAsync_returnValue() throws ExecutionException, InterruptedException {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.putAsync("key", "val2").toCompletableFuture().get());
    }

    @Test
    public void testPutIfAbsent_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.putIfAbsent("key", "val2"));
    }

    @Test
    public void testPutIfAbsentAsync_returnValue() throws ExecutionException, InterruptedException {
        assumeTrue(map instanceof MapProxyImpl);

        testEntryLoader.putExternally("key", "val", 5, TimeUnit.DAYS);
        assertEquals("val", ((MapProxyImpl<String, String>) map).putIfAbsentAsync("key", "val2").toCompletableFuture().get());
    }

    @Test
    public void testRemove_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.remove("key"));
    }

    @Test
    public void testRemoveIfSame_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertTrue(map.remove("key", "val"));
    }

    @Test
    public void testRemoveAsync_returnValue() throws ExecutionException, InterruptedException {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.removeAsync("key").toCompletableFuture().get());
    }

    @Test
    public void testReplace_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertEquals("val", map.replace("key", "val2"));
    }

    @Test
    public void testReplaceIfSame_returnValue() {
        testEntryLoader.putExternally("key", "val", 5 , TimeUnit.DAYS);
        assertTrue(map.replace("key", "val", "val2"));
    }

    @Test
    public void testKeySet() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Set<String> entries = map.keySet();
        for (int i = 0; i < entryCount; i++) {
            assertContains(entries, "key" + i);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testKeySet_withPredicate() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Set<String> entries = map.keySet(Predicates.greaterEqual("__key", "key90"));
        for (int i = 90; i < entryCount; i++) {
            assertContains(entries, "key" + i);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testValues() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Collection<String> entries = map.values();
        for (int i = 0; i < entryCount; i++) {
            assertContains(entries, "val" + i);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testValues_withPredicate() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Collection<String> entries = map.values(Predicates.greaterEqual("this", "val90"));
        for (int i = 90; i < entryCount; i++) {
            assertContains(entries, "val" + i);
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testEntrySet() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (int i = 0; i < entryCount; i++) {
            assertContains(entries, new AbstractMap.SimpleEntry<>("key" + i, "val" + i));
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    @Test
    public void testEntrySet_withPredicate() {
        final int entryCount = 100;
        putEntriesExternally(testEntryLoader, "key", "val", 10000, 0, entryCount);
        Set<Map.Entry<String, String>> entries = map.entrySet(Predicates.greaterEqual("__key", "key90"));
        for (int i = 90; i < entryCount; i++) {
            assertContains(entries, new AbstractMap.SimpleEntry<>("key" + i, "val" + i));
        }
        assertExpiredEventually(map, "key", 0, entryCount);
    }

    private void putEntriesExternally(TestEntryLoader loader, String keyPrefix, String valuePrefix, long expirationMillis, int from, int to) {
        long expirationTime = System.currentTimeMillis() + expirationMillis;
        for (int i = from; i < to; i++) {
            loader.putExternally(keyPrefix + i, valuePrefix + i, expirationTime);
        }
    }

    private void putEntriesExternally(TestEntryLoader loader, String keyPrefix, String valuePrefix, int from, int to) {
        for (int i = from; i < to; i++) {
            loader.putExternally(keyPrefix + i, valuePrefix + i);
        }
    }

    private void assertExpiredEventually(IMap map, String prefix, int from, int to) {
        assertTrueEventually(() -> {
            for (int i = from; i < to; i++) {
                EntryView entryView = map.getEntryView(prefix + i);
                assertNull("Current time: " + System.currentTimeMillis(), entryView);
            }
        });
    }

    private void assertInMemory(HazelcastInstance[] instances, String mapName, String key, String expectedValue) {
        InternalPartitionService partitionService = getPartitionService(instances[0]);
        int partitionId = partitionService.getPartitionId(key);
        Address partitionOwnerAddress = partitionService.getPartitionOwner(partitionId);

        HazelcastInstance owner = getInstance(instances, partitionOwnerAddress);

        InternalSerializationService serializationService = getSerializationService(owner);
        Data keyData = serializationService.toData(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(owner);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        Record record = recordStore.getRecordOrNull(keyData);
        Object actualValue = record == null ? null : serializationService.toObject(record.getValue());
        assertEquals(expectedValue, actualValue);
    }

    private HazelcastInstance getInstance(HazelcastInstance[] instances, Address address) {
        for (int i = 0; i < instances.length; i++) {
            HazelcastInstance candidate = instances[i];
            if (candidate.getCluster().getLocalMember().getAddress().equals(address)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Address " + address + " not found in the cluster");
    }

    @Test
    public void testLoadEntryAtCurrentTime() {
        testEntryLoader.putExternally("key", "value", 42);

        MapService service = getNodeEngineImpl(instances[0]).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Config config = mapServiceContext.getNodeEngine().getConfig();
        MapContainer mapContainer = new MapContainer("anyName", config, mapServiceContext);
        Data key = mapServiceContext.toData("key");
        DefaultRecordStore recordStore = new DefaultRecordStore(mapContainer, 0, mock(MapKeyLoader.class), mock(ILogger.class)) ;
        assertNull(recordStore.loadRecordOrNull(key, false, null));
    }
}
