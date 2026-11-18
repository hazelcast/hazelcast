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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.createChunksFromSnapshot;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkUtilTest {

    private final RaftIntegration raftIntegration = mock(RaftIntegration.class);
    private final ILogger logger = mock(ILogger.class);

    @Test
    public void testCreateChunksFromSnapshot_whenSnapshotIsChunkList_returnsSameList() {
        SnapshotChunk chunk = new SnapshotChunk();
        List<SnapshotChunk> originalChunks = Collections.singletonList(chunk);

        List<SnapshotChunk> result = createChunksFromSnapshot(raftIntegration, 1, 10,
                emptySet(), 0, originalChunks, logger);

        assertSame(originalChunks, result);
    }

    @Test
    public void testCreateChunksFromSnapshot_whenSnapshotIsEmptyMap_returnsEmptyList() {
        List<SnapshotChunk> chunks = createChunksFromSnapshot(raftIntegration, 1, 10,
                emptySet(), 0, new HashMap<>(), logger);

        assertTrue(chunks.isEmpty());
    }

    @Test
    public void testCreateChunksFromSnapshot_whenSnapshotIsNull_throwsAssertionError() {
        assertThrows(AssertionError.class, () ->
                createChunksFromSnapshot(raftIntegration, 1, 10, emptySet(), 0, null, logger));
    }

    @Test
    public void testCreateChunksFromSnapshot_whenSnapshotIsNotMap_throwsAssertionError() {
        assertThrows(AssertionError.class, () ->
                createChunksFromSnapshot(raftIntegration, 1, 10, emptySet(), 0, "not-a-map", logger));
    }
}
