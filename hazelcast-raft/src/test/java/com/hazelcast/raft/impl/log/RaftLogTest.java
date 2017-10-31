package com.hazelcast.raft.impl.log;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLogTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private RaftLog log;

    @Before
    public void setUp() throws Exception {
        log = new RaftLog();
    }

    @Test
    public void test_initialState() throws Exception {
        Assert.assertEquals(0, log.lastLogTerm());
        Assert.assertEquals(0, log.lastLogIndex());
    }

    @Test
    public void test_appendEntries_withSameTerm() throws Exception {
        log.appendEntries(new LogEntry(1, 1, null));
        log.appendEntries(new LogEntry(1, 2, null));
        LogEntry last = new LogEntry(1, 3, null);
        log.appendEntries(last);

        Assert.assertEquals(last.term(), log.lastLogTerm());
        Assert.assertEquals(last.index(), log.lastLogIndex());
    }

    @Test
    public void test_appendEntries_withDifferentTerms() throws Exception {
        LogEntry[] entries = new LogEntry[] {
            new LogEntry(1, 1, null),
            new LogEntry(1, 2, null),
            new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        Assert.assertEquals(last.term(), log.lastLogTerm());
        Assert.assertEquals(last.index(), log.lastLogIndex());

        LogEntry lastLogEntry = log.lastLogEntry();
        Assert.assertEquals(last.term(), lastLogEntry.term());
        Assert.assertEquals(last.index(), lastLogEntry.index());
    }

    @Test
    public void test_appendEntries_withLowerTerm() throws Exception {
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
    public void test_appendEntries_withLowerIndex() throws Exception {
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
    public void test_appendEntries_withEqualIndex() throws Exception {
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
    public void test_appendEntries_withGreaterIndex() throws Exception {
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
    public void getEntry() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        for (int i = 1; i <= log.lastLogIndex(); i++) {
            LogEntry entry = log.getEntry(i);
            Assert.assertEquals(1, entry.term());
            Assert.assertEquals(i, entry.index());
        }
    }

    @Test
    public void getEntry_withUnknownIndex() throws Exception {
        Assert.assertNull(log.getEntry(1));
    }

    @Test
    public void getEntry_withZeroIndex() throws Exception {
        exception.expect(IllegalArgumentException.class);
        log.getEntry(0);
    }

    @Test
    public void getEntriesBetween() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        LogEntry[] result = log.getEntriesBetween(1, 3);
        Assert.assertArrayEquals(entries, result);

        result = log.getEntriesBetween(1, 2);
        Assert.assertArrayEquals(Arrays.copyOfRange(entries, 0, 2), result);

        result = log.getEntriesBetween(2, 3);
        Assert.assertArrayEquals(Arrays.copyOfRange(entries, 1, 3), result);
    }

    @Test
    public void getEntriesBetween_outOfRange() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        LogEntry[] result = log.getEntriesBetween(4, 10);
        Assert.assertArrayEquals(new LogEntry[0], result);
    }

    @Test
    public void truncateEntriesFrom() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null)
        };
        log.appendEntries(entries);

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        Assert.assertEquals(2, truncated.size());
        Assert.assertArrayEquals(Arrays.copyOfRange(entries, 2, 4), truncated.toArray());

        for (int i = 1; i <= 2; i++) {
            Assert.assertEquals(entries[i - 1], log.getEntry(i));
        }
        Assert.assertNull(log.getEntry(3));
    }

    @Test
    public void truncateEntriesFrom_outOfRange() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
        };
        log.appendEntries(entries);

        List<LogEntry> truncated = log.truncateEntriesFrom(4);
        Assert.assertTrue(truncated.isEmpty());

        for (int i = 1; i <= entries.length; i++) {
            Assert.assertEquals(entries[i - 1], log.getEntry(i));
        }
    }

}
