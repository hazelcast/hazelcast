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

package com.hazelcast.test.starter.answer.test;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.collection.CollectionService;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapPartitionContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionServiceState;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AnswerTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        Assume.assumeTrue("This test ensures access to internals works with a previous minor version. "
                + "Test execution is skipped for new major versions.", CURRENT_CLUSTER_VERSION.getMinor() > 0);
        Config config = smallInstanceConfig()
                .setInstanceName("test-name");

        // we always want to ensure, that the access to internals work with a previous minor version
        hz = HazelcastStarter.newHazelcastInstance(PREVIOUS_CLUSTER_VERSION.toString(), config, false);
    }

    @After
    public void tearDown() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @Test
    public void testHazelcastInstanceImpl() {
        HazelcastInstanceImpl hazelcastInstance = HazelcastStarter.getHazelcastInstanceImpl(hz);

        LifecycleService lifecycleService = hazelcastInstance.getLifecycleService();
        assertNotNull("LifecycleService should not be null ", lifecycleService);
        assertTrue("Expected LifecycleService.isRunning() to be true", lifecycleService.isRunning());
    }

    @Test
    public void testNode() {
        Node node = HazelcastStarter.getNode(hz);
        assertNotNull("Node should not be null", node);
        assertNotNull("NodeEngine should not be null", node.getNodeEngine());
        assertNotNull("ClusterService should not be null", node.getClusterService());
        assertEquals("Expected NodeState to be ACTIVE", NodeState.ACTIVE, node.getState());
        assertTrue("Expected isRunning() to be true", node.isRunning());
        assertTrue("Expected isMaster() to be true", node.isMaster());
        Address localAddress = hz.getCluster().getLocalMember().getAddress();
        assertEquals("Expected the same address from HazelcastInstance and Node", localAddress, node.getThisAddress());
    }

    @Test
    public void testNodeEngine() {
        Node node = HazelcastStarter.getNode(hz);

        NodeEngineImpl nodeEngine = node.getNodeEngine();
        assertNotNull("NodeEngine should not be null", nodeEngine);

        HazelcastInstance hazelcastInstance = nodeEngine.getHazelcastInstance();
        assertNotNull("HazelcastInstance should not be null", hazelcastInstance);

        SerializationService serializationService = nodeEngine.getSerializationService();
        assertNotNull("SerializationService should not be null", serializationService);

        OperationServiceImpl operationService = nodeEngine.getOperationService();
        assertNotNull("InternalOperationService should not be null", operationService);

        CollectionService collectionService = nodeEngine.getService(SetService.SERVICE_NAME);
        assertNotNull("CollectionService from ISet should not be null", collectionService);

        collectionService = nodeEngine.getService(ListService.SERVICE_NAME);
        assertNotNull("CollectionService from IList should not be null", collectionService);

        MultiMapService multiMapService = nodeEngine.getService(MultiMapService.SERVICE_NAME);
        assertNotNull("MultiMapService should not be null", multiMapService);
    }

    @Test
    public void testClusterService() {
        Node node = HazelcastStarter.getNode(hz);
        ClusterServiceImpl clusterService = node.getClusterService();

        MemberImpl localMember = clusterService.getLocalMember();
        assertNotNull("localMember should not be null", localMember);
        assertTrue("Member should be the local member", localMember.localMember());
        assertFalse("Member should be no lite member", localMember.isLiteMember());
        assertEquals("Expected the same address from Node and local member", node.getThisAddress(), localMember.getAddress());

        MemberImpl member = clusterService.getMember(node.getThisAddress());
        assertEquals("Expected the same member via getMember(thisAddress) as the local member", localMember, member);
    }

    @Test
    public void testPartitionService() {
        Node node = HazelcastStarter.getNode(hz);
        InternalPartitionService partitionService = node.getPartitionService();
        int expectedPartitionCount = Integer.parseInt(hz.getConfig().getProperty(GroupProperty.PARTITION_COUNT.getName()));

        IPartition[] partitions = partitionService.getPartitions();
        assertNotNull("partitions should not be null", partitions);
        assertEqualsStringFormat("Expected %s partitions, but found %s", expectedPartitionCount, partitions.length);

        int partitionCount = partitionService.getPartitionCount();
        assertEqualsStringFormat("Expected partitionCount of %s, but was %s", expectedPartitionCount, partitionCount);

        InternalPartition partition = partitionService.getPartition(expectedPartitionCount / 2);
        assertNotNull("partition should not be null", partition);
        assertTrue("partition should be local", partition.isLocal());
        assertEquals("partition should be owned by this node", node.getThisAddress(), partition.getOwnerOrNull());

        PartitionServiceState partitionServiceState = getPartitionServiceState(hz);
        assertEquals("Expected SAFE PartitionServiceState (before shutdown)", PartitionServiceState.SAFE, partitionServiceState);

        hz.shutdown();

        partitionServiceState = getPartitionServiceState(hz);
        assertEquals("Expected SAFE PartitionServiceState (after shutdown)", PartitionServiceState.SAFE, partitionServiceState);
    }

    @Test
    public void testSerializationService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();

        int original = 42;
        Data data = serializationService.toData(original);
        assertNotNull("data should not be null", data);
        assertFalse("data should be no proxy class", isProxyClass(data.getClass()));
        assertEquals("toObject() should return original value", original,
                ((Integer) serializationService.toObject(data)).intValue());

        SerializationService localSerializationService = new DefaultSerializationServiceBuilder().build();
        Data localData = localSerializationService.toData(original);
        assertEquals("data should be the same as from local SerializationService", localData, data);
    }

    @Test
    public void testSetService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        SetService setService = nodeEngine.getService(SetService.SERVICE_NAME);

        assertEquals(SetService.SERVICE_NAME, setService.getServiceName());
        ConcurrentMap<String, ? extends CollectionContainer> containerMap = setService.getContainerMap();
        assertTrue("containerMap should be empty", containerMap.isEmpty());

        ISet<Object> set = hz.getSet("mySet");
        set.add(42);

        assertFalse("containerMap should be empty", containerMap.isEmpty());
        CollectionContainer container = containerMap.get("mySet");
        assertEquals("Expected one item in the collection container", 1, container.size());

        Collection<CollectionItem> collection = container.getCollection();
        assertEquals("Expected one primary item in the container", 1, collection.size());
        Map<Long, CollectionItem> backupMap = container.getMap();
        assertEquals("Expected one backup item in the container", 1, backupMap.size());

        Collection<CollectionItem> values = backupMap.values();
        Iterator<CollectionItem> iterator = values.iterator();
        assertNotNull("containerMap iterator should not be null", iterator);
        assertTrue("containerMap iterator should have a next item", iterator.hasNext());
        CollectionItem collectionItem = iterator.next();
        assertNotNull("collectionItem should not be null", collectionItem);
        Data dataValue = collectionItem.getValue();
        assertNotNull("collectionItem should have a value", dataValue);
        Object value = serializationService.toObject(dataValue);
        assertEquals("Expected collectionItem value to be 42", 42, value);

        assertTrue("set should contain 42", set.contains(42));
        set.clear();
        assertFalse("set should not contain 42", set.contains(42));

        set.destroy();
    }

    @Test
    public void testQueueService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        QueueService queueService = nodeEngine.getService(QueueService.SERVICE_NAME);

        IQueue<Object> queue = hz.getQueue("myQueue");
        queue.add(42);

        QueueContainer container = queueService.getOrCreateContainer("myQueue", false);
        assertNotNull("container should not be null", container);
        assertEquals("Expected one item in the queue container", 1, container.size());

        Collection<QueueItem> collection = container.getItemQueue();
        assertEquals("Expected one primary item in the container", 1, collection.size());
        Map<Long, QueueItem> backupMap = container.getBackupMap();
        assertEquals("Expected one backup item in the container", 1, backupMap.size());

        Collection<QueueItem> values = backupMap.values();
        Iterator<QueueItem> iterator = values.iterator();
        assertNotNull("backupMap iterator should not be null", iterator);
        assertTrue("backupMap iterator should have a next item", iterator.hasNext());
        QueueItem queueItem = iterator.next();
        assertNotNull("queueItem should not be null", queueItem);
        Data dataValue = queueItem.getData();
        assertNotNull("queueItem should have a value", dataValue);
        Object value = serializationService.toObject(dataValue);
        assertEquals("Expected collectionItem value to be 42", 42, value);

        assertTrue("queue should contain 42", queue.contains(42));
        queue.clear();
        assertFalse("queue should not contain 42", queue.contains(42));

        queue.destroy();
    }

    @Test
    public void testCacheService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        HazelcastInstanceImpl hazelcastInstance = HazelcastStarter.getHazelcastInstanceImpl(hz);

        SerializationService serializationService = nodeEngine.getSerializationService();
        String key = randomString();
        Data keyData = serializationService.toData(key);
        int partitionId = hz.getPartitionService().getPartition(key).getPartitionId();

        CachingProvider provider = createServerCachingProvider(hazelcastInstance);
        HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();

        Cache<String, Integer> cache = cacheManager.getCache("myCache");
        assertNull("cache should be null", cache);

        CacheConfig<String, Integer> cacheConfig = new CacheConfig<String, Integer>("myCache");
        cache = cacheManager.createCache("myCache", cacheConfig);
        assertNotNull("cache should not be null", cache);

        cacheManager.getCache("myCache");
        assertNotNull("cache should not be null", cache);
        cache.put(key, 23);

        // ICacheRecordStore access
        CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix("myCache");
        ICacheRecordStore recordStore = cacheService.getRecordStore(cacheNameWithPrefix, partitionId);
        assertNotNull("recordStore should not be null", recordStore);
        assertEquals("Expected one item in the recordStore", 1, recordStore.size());

        Object dataValue = recordStore.get(keyData, null);
        assertNotNull("dataValue should not be null", dataValue);
        Integer value = serializationService.toObject(dataValue);
        assertNotNull("Expected value not to be null", value);
        assertEquals("Expected the value to be 23", 23, (int) value);

        // EntryProcessor invocation
        Map<String, EntryProcessorResult<Integer>> resultMap;
        int result;

        result = cache.invoke(key, new IntegerValueEntryProcessor());
        assertEquals("Expected the result to be -23 (after invoke())", -23, result);

        result = cache.invoke(key, new IntegerValueEntryProcessor(), 42);
        assertEquals("Expected the value to be 42 (after invoke())", 42, result);

        resultMap = cache.invokeAll(Collections.singleton(key), new IntegerValueEntryProcessor());
        result = resultMap.get(key).get();
        assertEquals("Expected the value to be -23 (after invokeAll())", -23, result);

        resultMap = cache.invokeAll(Collections.singleton(key), new IntegerValueEntryProcessor(), 42);
        result = resultMap.get(key).get();
        assertEquals("Expected the value to be 42 (after invokeAll())", 42, result);

        // clear and destroy
        assertTrue("cache should contain key", cache.containsKey(key));
        cache.clear();
        assertFalse("cache should not contain key", cache.containsKey(key));

        cacheManager.destroyCache("myCache");
    }

    @Test
    public void testMapService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();

        SerializationService serializationService = nodeEngine.getSerializationService();
        String key = randomString();
        Data keyData = serializationService.toData(key);
        int partitionId = hz.getPartitionService().getPartition(key).getPartitionId();

        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        assertNotNull("mapServiceContext should not be null", mapServiceContext);

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        assertNotNull("partitionContainer should not be null", partitionContainer);

        RecordStore recordStore = partitionContainer.getExistingRecordStore("myMap");
        assertNull("recordStore should be null", recordStore);

        IMap<Object, Object> map = hz.getMap("myMap");
        map.put(key, 23);

        recordStore = partitionContainer.getExistingRecordStore("myMap");
        assertNotNull("recordStore should not be null", recordStore);
        assertEquals("Expected one item in the recordStore", 1, recordStore.size());

        Object dataValue = recordStore.get(keyData, true, null);
        assertNotNull("dataValue should not be null", dataValue);
        Integer value = serializationService.toObject(dataValue);
        assertNotNull("Expected value not to be null", value);
        assertEquals("Expected value to be 23", 23, (int) value);

        assertTrue("map should contain key", map.containsKey(key));
        map.clear();
        assertFalse("map should not contain key", map.containsKey(key));

        map.destroy();
    }

    @Test
    public void testMultiMapService() {
        Node node = HazelcastStarter.getNode(hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        MultiMapService multiMapService = nodeEngine.getService(MultiMapService.SERVICE_NAME);

        SerializationService serializationService = nodeEngine.getSerializationService();
        String key = randomString();
        Data keyData = serializationService.toData(key);
        int partitionId = hz.getPartitionService().getPartition(key).getPartitionId();

        MultiMap<String, String> multiMap = hz.getMultiMap("myMultiMap");
        multiMap.put(key, "value1");
        multiMap.put(key, "value2");

        MultiMapPartitionContainer partitionContainer = multiMapService.getPartitionContainer(partitionId);
        MultiMapContainer multiMapContainer = partitionContainer.getMultiMapContainer("myMultiMap", false);

        ConcurrentMap<Data, MultiMapValue> multiMapValues = multiMapContainer.getMultiMapValues();
        for (Map.Entry<Data, MultiMapValue> entry : multiMapValues.entrySet()) {
            Data actualKeyData = entry.getKey();
            MultiMapValue multiMapValue = entry.getValue();

            String actualKey = serializationService.toObject(actualKeyData);
            assertEquals(keyData, actualKeyData);
            assertEquals(key, actualKey);

            Collection<MultiMapRecord> collection = multiMapValue.getCollection(false);
            Collection<String> actualValues = new ArrayList<String>(collection.size());
            for (MultiMapRecord record : collection) {
                String value = serializationService.toObject(record.getObject());
                actualValues.add(value);
            }
            assertEquals("MultiMapValue should contain 2 MultiMapRecords", 2, actualValues.size());
            assertTrue("MultiMapValue should contain value1", actualValues.contains("value1"));
            assertTrue("MultiMapValue should contain value2", actualValues.contains("value2"));
        }

        assertTrue("multiMap should contain key", multiMap.containsKey(key));
        multiMap.clear();
        assertFalse("multiMap should not contain key", multiMap.containsKey(key));

        multiMap.destroy();
    }

    private static class IntegerValueEntryProcessor implements EntryProcessor<String, Integer, Integer>, Serializable {

        @Override
        public Integer process(MutableEntry<String, Integer> entry, Object... arguments) throws EntryProcessorException {
            if (arguments.length > 0) {
                return (Integer) arguments[0];
            }
            return -entry.getValue();
        }
    }
}
