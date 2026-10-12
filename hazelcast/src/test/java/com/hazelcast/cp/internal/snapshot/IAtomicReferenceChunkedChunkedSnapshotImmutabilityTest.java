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

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRef;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefSnapshot;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.datastructures.snapshot.ValueDataChunk;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
public class IAtomicReferenceChunkedChunkedSnapshotImmutabilityTest
        extends ChunkedSnapshotImmutabilityTest<ValueDataChunk, AtomicRefSnapshot, AtomicRefService> {

    private NodeEngineImpl nodeEngine;
    private CPGroupId groupId;

    @Before
    public void setUp() {
        this.nodeEngine = mockNodeEngineImplForChunking();
        this.groupId = new RaftGroupId("atomicRefGroup", 1, 1);
    }

    @Override
    protected AtomicRefService newService() {
        AtomicRefService service = new AtomicRefService(nodeEngine);
        service.init(nodeEngine, new Properties());
        return service;
    }

    @Override
    protected CPGroupId groupId() {
        return groupId;
    }

    @Override
    protected void populateInitialState(AtomicRefService service, CPGroupId groupId) {
        AtomicRef ref1 = service.getAtomicValue(groupId, "ref1");
        ref1.set(data("v1"));

        AtomicRef ref2 = service.getAtomicValue(groupId, "ref2");
        ref2.set(data("v2"));
    }

    @Override
    protected void mutateService(AtomicRefService service, CPGroupId groupId) {
        AtomicRef ref1 = service.getAtomicValue(groupId, "ref1");
        ref1.set(data("v1-updated"));

        AtomicRef ref3 = service.getAtomicValue(groupId, "ref3");
        ref3.set(data("v3"));
    }

    @Override
    protected Object chunkGroupFingerprint(DataChunkGroup<ValueDataChunk> group) {
        Map<String, Object> map = new TreeMap<>();

        for (ValueDataChunk chunk : group.getServiceData()) {
            String name = chunk.getName();
            if (chunk.isDestroyed()) {
                map.put(name, "D");
            } else {
                map.put(name, chunk.getValue());
            }
        }

        return map;
    }

    private NodeEngineImpl mockNodeEngineImplForChunking() {
        // AbstractCPMigrationAwareService casts the NodeEngine to NodeEngineImpl.
        NodeEngineImpl engine = mock(NodeEngineImpl.class);

        // Config with CP subsystem enabled
        Config config = new Config();
        CPSubsystemConfig cpConfig = new CPSubsystemConfig();
        cpConfig.setCPMemberCount(3);
        config.setCPSubsystemConfig(cpConfig);
        when(engine.getConfig()).thenReturn(config);

        // Properties
        HazelcastProperties hzProps = new HazelcastProperties(new Properties());
        when(engine.getProperties()).thenReturn(hzProps);

        // Metrics
        MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
        when(engine.getMetricsRegistry()).thenReturn(metricsRegistry);

        // RaftService used by RaftAtomicValueService.init()
        RaftService raftService = mock(RaftService.class);
        when(raftService.isCpSubsystemEnabled()).thenReturn(true);
        when(engine.getService(RaftService.SERVICE_NAME)).thenReturn(raftService);

        return engine;
    }
}
