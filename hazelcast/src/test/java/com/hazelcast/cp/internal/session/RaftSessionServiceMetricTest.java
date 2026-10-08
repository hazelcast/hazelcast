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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceMetricTest.DummyCollector;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceMetricTest.DummyMetricDescriptor;
import com.hazelcast.cp.session.CPSession.CPSessionOwnerType;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Map;

import static com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceMetricTest.cpGroup1;
import static com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceMetricTest.cpGroup2;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_SESSION_COUNT;
import static org.junit.Assert.assertEquals;

public class RaftSessionServiceMetricTest {

    @Test
    public void testSessionCountMetrics_empty() {
        var descriptor = new DummyMetricDescriptor();
        var collector = new DummyCollector();

        RaftSessionService.addSessionMetrics(descriptor, collector, Map.of());

        assertEquals(0, collector.getEntries().size());
    }

    @Test
    public void testSessionCountMetrics_singleCpGroup() throws UnknownHostException {
        RaftSessionRegistry registry = registryWithSessions(cpGroup1, 2);

        var descriptor = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        RaftSessionService.addSessionMetrics(descriptor, collector, Map.of(cpGroup1, registry));

        assertEquals(2L, singleValue(collector, CP_METRIC_SESSION_COUNT, cpGroup1.getName()));
    }

    @Test
    public void testSessionCountMetrics_multipleCpGroups() throws UnknownHostException {
        RaftSessionRegistry group1Registry = registryWithSessions(cpGroup1, 2);
        RaftSessionRegistry group2Registry = registryWithSessions(cpGroup2, 3);

        var descriptor = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        RaftSessionService.addSessionMetrics(descriptor, collector, Map.of(cpGroup1, group1Registry, cpGroup2, group2Registry));

        assertEquals(2L, singleValue(collector, CP_METRIC_SESSION_COUNT, cpGroup1.getName()));
        assertEquals(3L, singleValue(collector, CP_METRIC_SESSION_COUNT, cpGroup2.getName()));
    }

    @Test
    public void testSessionCountMetrics_sessionClosed() throws UnknownHostException {
        RaftSessionRegistry registry = registryWithSessions(cpGroup1, 2);
        registry.closeSession(1L);

        var descriptor = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        RaftSessionService.addSessionMetrics(descriptor, collector, Map.of(cpGroup1, registry));

        assertEquals(1L, singleValue(collector, CP_METRIC_SESSION_COUNT, cpGroup1.getName()));
    }

    private static RaftSessionRegistry registryWithSessions(CPGroupId groupId, int sessionCount) throws UnknownHostException {
        RaftSessionRegistry registry = new RaftSessionRegistry(groupId);
        Address endpoint = new Address("localhost", 1111);
        for (int i = 0; i < sessionCount; i++) {
            registry.createNewSession(30_000L, endpoint, "server1", CPSessionOwnerType.SERVER, System.currentTimeMillis());
        }
        return registry;
    }

    private static long singleValue(DummyCollector collector, String metric, String discriminatorValue) {
        return collector.getEntries().stream()
                .filter(e -> e.descriptor().metric().equals(metric) && e.descriptor().discriminatorValue().equals(discriminatorValue))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No metric [" + metric + "@" + discriminatorValue + "] collected"))
                .value();
    }
}
