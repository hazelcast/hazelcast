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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheIMapEventHandlingTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private HazelcastInstance member;

    private String mapName;
    private IMap<Integer, Integer> map;
    private QueryCache<Integer, Integer> queryCache;

    @Before
    public void setUp() {
        member = createHazelcastInstance();

        mapName = randomMapName();
        map = getMap(member, mapName);
        queryCache = map.getQueryCache("cqc", TRUE_PREDICATE, true);
    }

    @Test
    public void testEvent_MERGED() throws Exception {
        final int key = 1;
        final int existingValue = 1;
        final int mergingValue = 2;

        map.put(key, existingValue);

        executeMergeOperation(member, mapName, key, mergingValue);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Integer currentValue = queryCache.get(key);
                assertEquals(mergingValue, (Object) currentValue);
            }
        });
    }

    private void executeMergeOperation(HazelcastInstance member, String mapName, int key, int mergedValue) throws Exception {
        Node node = getNode(member);
        NodeEngineImpl nodeEngine = node.nodeEngine;
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        SerializationService serializationService = getSerializationService(member);

        Data keyData = serializationService.toData(key);
        Data valueData = serializationService.toData(mergedValue);
        SplitBrainMergeTypes.MapMergeTypes mergingEntry
                = createMergingEntry(serializationService, keyData, valueData,
                Mockito.mock(Record.class), ExpiryMetadata.NULL);

        Operation mergeOperation = new MergeOperation(mapName, singletonList(mergingEntry),
                new PassThroughMergePolicy<>(), false);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        Future<Object> future = operationService.invokeOnPartition(SERVICE_NAME, mergeOperation, partitionId);
        future.get();
    }

    @Test
    public void testEvent_EXPIRED() throws Exception {
        int key = 1;
        int value = 1;

        final CountDownLatch latch = new CountDownLatch(1);
        queryCache.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }, true);

        map.put(key, value, 1, SECONDS);

        latch.await();
        sleepSeconds(1);

        // map#get creates EXPIRED event
        map.get(key);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, queryCache.size());
            }
        });
    }

    @Test
    public void testListenerRegistration() {
        UUID addEntryListener = queryCache.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        UUID removeEntryListener = queryCache.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        assertFalse(queryCache.removeEntryListener(UUID.randomUUID()));

        assertTrue(queryCache.removeEntryListener(removeEntryListener));
        assertFalse(queryCache.removeEntryListener(removeEntryListener));

        assertTrue(queryCache.removeEntryListener(addEntryListener));
        assertFalse(queryCache.removeEntryListener(addEntryListener));
    }
}
