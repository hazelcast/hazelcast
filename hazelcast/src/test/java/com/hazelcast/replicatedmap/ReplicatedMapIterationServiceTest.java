/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IterationResult;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.iterator.ReplicatedMapIterationService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.replicatedmap.impl.iterator.ReplicatedMapIterationService.ITERATOR_CLEANUP_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapIterationServiceTest extends HazelcastTestSupport {

    public static final int ITEM_COUNT = 1000;
    private ReplicatedMapIterationService service;
    private SerializationService ss;
    private HazelcastInstance instance;

    @Before
    public void setUp() {
        instance = createHazelcastInstance(getConfig());
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        ss = nodeEngine.getSerializationService();
        service = new ReplicatedMapIterationService(nodeEngine.getService(ReplicatedMapService.SERVICE_NAME), ss, nodeEngine);
    }

    @Override
    protected Config getConfig() {
        Config c = super.getConfig();
        c.getProperties().setProperty(ITERATOR_CLEANUP_TIMEOUT_MILLIS.getName(), "1000");
        return c;
    }

    @Test
    public void emptyReplicatedMap_createIterator() {
        String name = randomName();
        instance.getReplicatedMap(name);
        service.createIterator(name, 0, UuidUtil.newUnsecureUUID());
    }

    @Test
    public void nonExistingReplicatedMap_createIterator_throws() {
        String name = randomName();
        assertThatThrownBy(() -> service.createIterator(name, 0, UuidUtil.newUnsecureUUID())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("There is no ReplicatedRecordStore for " + name);
    }

    @Test
    public void removeStaleIterators_cleansStaleIterators() throws InterruptedException {
        String name = randomName();
        populateMap(instance.getReplicatedMap(name));
        UUID uuid = UuidUtil.newUnsecureUUID();
        service.createIterator(name, 0, uuid);
        assertThat(service.getIteratorManager().getKeySet()).containsExactlyInAnyOrder(uuid);
        Thread.sleep(1500);
        service.removeStaleIterators();
        assertThat(service.getIteratorManager().getKeySet()).isEmpty();
    }

    @Test
    public void iterate() {
        String name = randomName();
        populateMap(instance.getReplicatedMap(name));
        int partitionId = 12;
        UUID iteratorId = UuidUtil.newUnsecureUUID();
        service.createIterator(name, partitionId, iteratorId);
        int pageSize = 100;
        iterateAndAssert(iteratorId, pageSize, partitionId);
    }

    private void iterateAndAssert(UUID cursorId, int pageSize, int partitionId) {
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            if (instance.getPartitionService().getPartition(i).getPartitionId() == partitionId) {
                keys.add(i);
            }
        }
        IterationResult<ReplicatedMapEntryViewHolder> result = service.iterate(cursorId, pageSize);
        // first iteration does not forget a cursor id
        assertThat(result.getCursorIdToForget()).isEqualTo(null);
        assertThat(result.getCursorId()).isNotNull();
        assertThat(result.getCursorId()).isNotEqualTo(cursorId);
        List<Integer> actualKeys = new ArrayList<>();
        assertThat(result.getPage()).hasSizeLessThanOrEqualTo(pageSize);
        for (ReplicatedMapEntryViewHolder entry : result.getPage()) {
            int key = ss.toObject(entry.getKey());
            int value = ss.toObject(entry.getValue());
            assertThat(key).isEqualTo(value);
            actualKeys.add(key);
        }
        assertThat(actualKeys).containsExactlyInAnyOrderElementsOf(keys);
    }

    private void populateMap(Map<Integer, Integer> map) {
        Map<Integer, Integer> hashMap = new HashMap<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            hashMap.put(i, i);
        }
        map.putAll(hashMap);
    }
}
