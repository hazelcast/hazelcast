/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.log.RaftLog.newRaftLog;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftLogTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private RaftLog log;

    @Before
    public void setUp() throws Exception {
        log = newRaftLog(100);
    }

    @Test
    public void test_initialState() {
        assertEquals(0, log.lastLogOrSnapshotTerm());
        assertEquals(0, log.lastLogOrSnapshotIndex());
    }

    @Test
    public void test_appendEntries_withSameTerm() {
        log.appendEntries(new LogEntry(1, 1, null));
        log.appendEntries(new LogEntry(1, 2, null));
        LogEntry last = new LogEntry(1, 3, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogOrSnapshotTerm());
        assertEquals(last.index(), log.lastLogOrSnapshotIndex());
    }

    @Test
    public void test_appendEntries_withHigherTerms() {
        LogEntry[] entries = new LogEntry[] {
            new LogEntry(1, 1, null),
            new LogEntry(1, 2, null),
            new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogOrSnapshotTerm());
        assertEquals(last.index(), log.lastLogOrSnapshotIndex());

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertEquals(last.term(), lastLogEntry.term());
        assertEquals(last.index(), lastLogEntry.index());
    }

    @Test
    public void test_appendEntries_withLowerTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntries_withLowerIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntries_withEqualIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntries_withGreaterIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withSameTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        LogEntry last = new LogEntry(1, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogOrSnapshotTerm());
        assertEquals(last.index(), log.lastLogOrSnapshotIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withHigherTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogOrSnapshotTerm());
        assertEquals(last.index(), log.lastLogOrSnapshotIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withEqualIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withGreaterIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(2, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void getEntry() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        for (int i = 1; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void getEntryAfterSnapshot() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.<RaftEndpoint>emptySet()));

        log.appendEntries(new LogEntry(1, 4, null));
        log.appendEntries(new LogEntry(1, 5, null));

        for (int i = 1; i <= 3; i++) {
            assertNull(log.getLogEntry(i));
        }

        for (int i = 4; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void getEntry_withUnknownIndex() {
        assertNull(log.getLogEntry(1));
    }

    @Test
    public void getEntry_withZeroIndex() {
        exception.expect(IllegalArgumentException.class);
        log.getLogEntry(0);
    }

    @Test
    public void getEntriesBetween() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        LogEntry[] result = log.getEntriesBetween(1, 3);
        assertArrayEquals(entries, result);

        result = log.getEntriesBetween(1, 2);
        assertArrayEquals(Arrays.copyOfRange(entries, 0, 2), result);

        result = log.getEntriesBetween(2, 3);
        assertArrayEquals(Arrays.copyOfRange(entries, 1, 3), result);
    }

    @Test
    public void getEntriesBetweenAfterSnapshot() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.<RaftEndpoint>emptySet()));

        LogEntry[] result = log.getEntriesBetween(3, 3);
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 3), result);
    }

    @Test
    public void getEntriesBetweenBeforeSnapshotIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.getEntriesBetween(2, 3);
    }

    @Test
    public void truncateEntriesFrom() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null)
        };
        log.appendEntries(entries);

        List<LogEntry> truncated = log.deleteEntriesFrom(3);
        assertEquals(2, truncated.size());
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 4), truncated.toArray());

        for (int i = 1; i <= 2; i++) {
            assertEquals(entries[i - 1], log.getLogEntry(i));
        }

        assertNull(log.getLogEntry(3));
    }

    @Test
    public void truncateEntriesFrom_afterSnapshot() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.<RaftEndpoint>emptySet()));

        List<LogEntry> truncated = log.deleteEntriesFrom(3);
        assertEquals(2, truncated.size());
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 4), truncated.toArray());

        assertNull(log.getLogEntry(3));
    }

    @Test
    public void truncateEntriesFrom_outOfRange() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.deleteEntriesFrom(4);
    }

    @Test
    public void truncateEntriesFrom_beforeSnapshotIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                };
        log.appendEntries(entries);
        log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.<RaftEndpoint>emptySet()));

        exception.expect(IllegalArgumentException.class);
        log.deleteEntriesFrom(1);
    }

    @Test
    public void setSnapshotAtLastLogIndex_forSingleEntryLog() {
        LogEntry[] entries = new LogEntry[] { new LogEntry(1, 1, null) };
        log.appendEntries(entries);
        Object snapshot = new Object();
        log.setSnapshot(new SnapshotEntry(1, 1, snapshot, 0, Collections.<RaftEndpoint>emptySet()));

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertEquals(1, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertEquals(log.lastLogOrSnapshotIndex(), 1);
        assertEquals(log.lastLogOrSnapshotTerm(), 1);
        assertEquals(log.snapshotIndex(), 1);

        LogEntry snapshotEntry = log.snapshot();
        assertEquals(1, snapshotEntry.index());
        assertEquals(1, snapshotEntry.term());
        assertEquals(snapshot, snapshotEntry.operation());

    }

    @Test
    public void setSnapshotAtLastLogIndex_forMultiEntryLog() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        log.setSnapshot(new SnapshotEntry(1, 5, null, 0, Collections.<RaftEndpoint>emptySet()));

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertEquals(log.lastLogOrSnapshotIndex(), 5);
        assertEquals(log.lastLogOrSnapshotTerm(), 1);
        assertEquals(log.snapshotIndex(), 5);

        LogEntry snapshot = log.snapshot();
        assertEquals(5, snapshot.index());
        assertEquals(1, snapshot.term());
    }

    @Test
    public void setSnapshot() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        int truncated = log.setSnapshot(new SnapshotEntry(1, 3, null, 0, Collections.<RaftEndpoint>emptySet()));
        assertEquals(3, truncated);

        for (int i = 1; i <= 3 ; i++) {
            assertFalse(log.containsLogEntry(i));
            assertNull(log.getLogEntry(i));
        }
        for (int i = 4; i <= 5 ; i++) {
            assertTrue(log.containsLogEntry(i));
            assertNotNull(log.getLogEntry(i));
        }

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertSame(lastLogEntry, log.getLogEntry(lastLogEntry.index()));
        assertEquals(log.lastLogOrSnapshotIndex(), 5);
        assertEquals(log.lastLogOrSnapshotTerm(), 1);
        assertEquals(log.snapshotIndex(), 3);

        LogEntry snapshot = log.snapshot();
        assertEquals(3, snapshot.index());
        assertEquals(1, snapshot.term());
    }

    @Test
    public void setSnapshot_multipleTimes() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        int truncated = log.setSnapshot(new SnapshotEntry(1, 2, null, 0, Collections.<RaftEndpoint>emptySet()));
        assertEquals(2, truncated);

        for (int i = 1; i <= 2 ; i++) {
            assertFalse(log.containsLogEntry(i));
            assertNull(log.getLogEntry(i));
        }
        for (int i = 3; i <= 5 ; i++) {
            assertTrue(log.containsLogEntry(i));
            assertNotNull(log.getLogEntry(i));
        }

        Object snapshot = new Object();
        truncated = log.setSnapshot(new SnapshotEntry(1, 4, snapshot, 0, Collections.<RaftEndpoint>emptySet()));
        assertEquals(2, truncated);

        for (int i = 1; i <= 4 ; i++) {
            assertFalse(log.containsLogEntry(i));
            assertNull(log.getLogEntry(i));
        }
        assertTrue(log.containsLogEntry(5));
        assertNotNull(log.getLogEntry(5));

        LogEntry lastLogEntry = log.lastLogOrSnapshotEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertSame(lastLogEntry, log.getLogEntry(lastLogEntry.index()));
        assertEquals(log.lastLogOrSnapshotIndex(), 5);
        assertEquals(log.lastLogOrSnapshotTerm(), 1);
        assertEquals(log.snapshotIndex(), 4);

        LogEntry snapshotEntry = log.snapshot();
        assertEquals(4, snapshotEntry.index());
        assertEquals(1, snapshotEntry.term());
        assertEquals(snapshotEntry.operation(), snapshot);
    }

}
