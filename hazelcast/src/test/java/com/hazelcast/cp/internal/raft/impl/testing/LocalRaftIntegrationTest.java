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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRaftIntegrationTest {

    @Test
    public void testTakeGroupSnapshotChunks_whenServiceReturnsNullSnapshot_returnsEmptyList() {
        SnapshotAwareService<?> service = mock(SnapshotAwareService.class);
        when(service.takeSnapshot(any(), anyLong())).thenReturn(null);

        ServiceInfo serviceInfo = new ServiceInfo("testService", service);
        TestRaftEndpoint endpoint = new TestRaftEndpoint(UuidUtil.newUnsecureUUID(), 5000);
        TestRaftGroupId groupId = new TestRaftGroupId("testGroup");

        LocalRaftIntegration integration = new LocalRaftIntegration(endpoint, groupId, serviceInfo, false);

        List<SnapshotChunk> chunks = integration.takeGroupSnapshotChunks(1, 10, Collections.emptySet(), 0);

        assertNotNull(chunks);
        assertTrue(chunks.isEmpty());
    }
}
