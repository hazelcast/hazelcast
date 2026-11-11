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

package com.hazelcast.cp.internal.raft.impl.log;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotEntryTest {

    @Test
    public void testSnapshotChunks_whenDefaultConstructor_returnsEmptyList() {
        SnapshotEntry entry = new SnapshotEntry();
        assertTrue(entry.snapshotChunks().isEmpty());
    }

    @Test
    public void testSnapshotChunks_whenNullOperation_returnsEmptyList() {
        SnapshotEntry entry = new SnapshotEntry(1, 10, null, 0, emptySet());
        assertEquals(emptyList(), entry.snapshotChunks());
    }

    @Test
    public void testSnapshotChunks_whenListOperation_returnsChunks() {
        SnapshotChunk chunk = new SnapshotChunk();
        List<SnapshotChunk> chunks = Collections.singletonList(chunk);
        SnapshotEntry entry = new SnapshotEntry(1, 10, chunks, 0, emptySet());

        assertSame(chunks, entry.snapshotChunks());
    }

    @Test
    public void testSnapshotChunks_whenNonListOperation_throwsIllegalStateException() {
        SnapshotEntry entry = new SnapshotEntry(1, 10, new HashMap<>(), 0, emptySet());

        IllegalStateException exception = assertThrows(IllegalStateException.class, entry::snapshotChunks);
        assertTrue(exception.getMessage().contains("Unexpected snapshot operation type"));
    }
}
