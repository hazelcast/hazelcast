/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.snapshot.NonChunkedSnapshotImmutabilityTest;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

@RunWith(HazelcastParallelClassRunner.class)
public class CountDownLatchServiceSnapshotImmutabilityTest
        extends NonChunkedSnapshotImmutabilityTest<CountDownLatchRegistry, CountDownLatchService> {

    private NodeEngineImpl nodeEngine;
    private CPGroupId groupId;

    @Before
    public void setUp() {
        this.nodeEngine = mockNodeEngine();
        this.groupId = new RaftGroupId("countDownLatchGroup", 1, 1);
    }

    @Override
    protected CountDownLatchService newService() {
        CountDownLatchService service = new CountDownLatchService(nodeEngine);
        service.init(nodeEngine, new Properties());
        return service;
    }

    @Override
    protected CPGroupId groupId() {
        return groupId;
    }

    @Override
    protected void populateInitialState(CountDownLatchService service, CPGroupId groupId) {
        // one live latch, one destroyed latch
        service.trySetCount(groupId, "latch1", 3);

        service.trySetCount(groupId, "latch2", 1);
        service.destroyRaftObject(groupId, "latch2");
    }

    @Override
    protected void mutateService(CountDownLatchService service, CPGroupId groupId) {
        service.countDown(groupId, "latch1", UUID.randomUUID(), 1);

        service.trySetCount(groupId, "latch3", 2);
        service.destroyRaftObject(groupId, "latch3");
    }

    @Override
    protected Object snapshotFingerprint(CountDownLatchRegistry snapshot) {
        Map<String, Object> fingerprint = new TreeMap<>();
        Map<String, Map<String, Object>> live = new TreeMap<>();
        for (Map.Entry<String, CountDownLatch> e : snapshot.getResources().entrySet()) {
            String name = e.getKey();
            CountDownLatch latch = e.getValue();

            Map<String, Object> state = new TreeMap<>();
            state.put("count", latch.getCount());
            state.put("round", latch.getRound());
            // not including latch.getRemainingCount() on purpose as it is transient.
            live.put(name, state);
        }
        fingerprint.put("live", live);

        // Destroyed latches by name
        fingerprint.put("destroyed", new TreeSet<>(snapshot.getDestroyed()));
        return fingerprint;
    }
}
