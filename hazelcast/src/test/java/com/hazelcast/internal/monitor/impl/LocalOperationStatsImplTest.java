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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalOperationStatsImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl();

        assertEquals(Long.MAX_VALUE, localOperationStats.getMaxVisibleSlowOperationCount());
        assertEquals(0, localOperationStats.getSlowOperations().size());
        assertTrue(localOperationStats.getCreationTime() > 0);
        assertNotNull(localOperationStats.toString());
    }

    @Test
    public void testNodeConstructor() {
        Config config = new Config();
        config.setProperty(ClusterProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT.getName(), "139");

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl(node);

        assertEquals(139, localOperationStats.getMaxVisibleSlowOperationCount());
        assertEquals(0, localOperationStats.getSlowOperations().size());
        assertTrue(localOperationStats.getCreationTime() > 0);
        assertNotNull(localOperationStats.toString());
    }

    @Test
    public void testSerialization() {
        Config config = new Config();
        config.setProperty(ClusterProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT.getName(), "127");

        SlowOperationInvocationDTO slowOperationInvocationDTO = new SlowOperationInvocationDTO();
        slowOperationInvocationDTO.id = 12345;
        slowOperationInvocationDTO.durationMs = 15000;
        slowOperationInvocationDTO.startedAt = 12381912;
        slowOperationInvocationDTO.operationDetails = "TestOperationDetails";

        List<SlowOperationInvocationDTO> invocationList = new ArrayList<SlowOperationInvocationDTO>();
        invocationList.add(slowOperationInvocationDTO);

        SlowOperationDTO slowOperationDTO = new SlowOperationDTO();
        slowOperationDTO.operation = "TestOperation";
        slowOperationDTO.stackTrace = "stackTrace";
        slowOperationDTO.totalInvocations = 4;
        slowOperationDTO.invocations = invocationList;

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        LocalOperationStatsImpl localOperationStats = new LocalOperationStatsImpl(node);
        localOperationStats.getSlowOperations().add(slowOperationDTO);

        LocalOperationStatsImpl deserialized = new LocalOperationStatsImpl();
        deserialized.fromJson(localOperationStats.toJson());

        assertEquals(localOperationStats.getCreationTime(), deserialized.getCreationTime());
        assertEquals(localOperationStats.getMaxVisibleSlowOperationCount(), deserialized.getMaxVisibleSlowOperationCount());
        assertEqualsSlowOperationDTOs(localOperationStats.getSlowOperations(), deserialized.getSlowOperations());
    }

    static void assertEqualsSlowOperationDTOs(List<SlowOperationDTO> slowOperations1, List<SlowOperationDTO> slowOperations2) {
        if (slowOperations1 == null) {
            assertNull(slowOperations2);
        } else {
            assertNotNull(slowOperations2);
        }
        assertEquals(slowOperations1.size(), slowOperations2.size());

        Iterator<SlowOperationDTO> iterator = slowOperations2.iterator();
        for (SlowOperationDTO slowOperationDTO1 : slowOperations1) {
            SlowOperationDTO slowOperationDTO2 = iterator.next();
            assertEquals(slowOperationDTO1.operation, slowOperationDTO2.operation);
            assertEquals(slowOperationDTO1.stackTrace, slowOperationDTO2.stackTrace);
            assertEquals(slowOperationDTO1.totalInvocations, slowOperationDTO2.totalInvocations);
            assertEquals(slowOperationDTO1.invocations.size(), slowOperationDTO2.invocations.size());
        }
    }
}
