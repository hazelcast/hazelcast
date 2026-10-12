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

package com.hazelcast.cp.internal.snapshot;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLong;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongSnapshot;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;


@RunWith(HazelcastParallelClassRunner.class)
public class AtomicLongSnapshotImmutabilityTest
        extends NonChunkedSnapshotImmutabilityTest<AtomicLongSnapshot, AtomicLongService> {

    private NodeEngineImpl nodeEngine;
    private CPGroupId groupId;

    @Before
    public void setUp() {
        this.nodeEngine = mockNodeEngine();
        this.groupId = new RaftGroupId("atomicLongGroup", 1, 1);
    }

    @Override
    protected AtomicLongService newService() {
        AtomicLongService service = new AtomicLongService(nodeEngine);
        service.init(nodeEngine, new Properties());
        return service;
    }

    @Override
    protected CPGroupId groupId() {
        return groupId;
    }

    @Override
    protected void populateInitialState(AtomicLongService service, CPGroupId groupId) {
        AtomicLong l1 = service.getAtomicValue(groupId, "long1");
        l1.getAndSet(1L);

        AtomicLong l2 = service.getAtomicValue(groupId, "long2");
        l2.getAndSet(2L);

        // Makes destroyed set non-empty at snapshot time
        service.destroyRaftObject(groupId, "long2");
    }

    @Override
    protected void mutateService(AtomicLongService service, CPGroupId groupId) {
        // increment and destroy one value
        AtomicLong l1 = service.getAtomicValue(groupId, "long1");
        l1.getAndAdd(1L); // 2

        AtomicLong l3 = service.getAtomicValue(groupId, "long3");
        l3.getAndAdd(3L);

        service.destroyRaftObject(groupId, "long2");
    }

    @Override
    protected Object snapshotFingerprint(AtomicLongSnapshot snapshot) {
        Map<String, Object> map = new TreeMap<>();

        for (Map.Entry<String, Long> e : snapshot.getValues()) {
            map.put("L:" + e.getKey(), e.getValue());
        }
        for (String name : snapshot.getDestroyed()) {
            map.put("D:" + name, Boolean.TRUE);
        }
        return map;
    }
}
