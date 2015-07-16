package com.hazelcast.transaction.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TransactionLogTest {

    @Test
    public void iterator() {
        TransactionLog log = new TransactionLog();
        TransactionLogRecord record1 = mock(TransactionLogRecord.class);
        TransactionLogRecord record2 = mock(TransactionLogRecord.class);

        log.add(record1);
        log.add(record2);

        Iterator<TransactionLogRecord> it = log.iterator();
        assertSame(record1, it.next());
        assertSame(record2, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void add_whenKeyAware() {
        TransactionLog log = new TransactionLog();
        KeyAwareTransactionLogRecord record = mock(KeyAwareTransactionLogRecord.class);
        String key = "foo";
        when(record.getKey()).thenReturn(key);

        log.add(record);

        assertSame(record, log.get(key));
        assertEquals(1, log.size());
    }

    @Test
    public void add_whenNotKeyAware() {
        TransactionLog log = new TransactionLog();
        TransactionLogRecord record = mock(TransactionLogRecord.class);

        log.add(record);

        assertEquals(1, log.size());
        assertEquals(asList(record), log.getRecordList());
    }

    @Test
    public void add_whenOverwrite() {
        TransactionLog log = new TransactionLog();
        String key = "foo";
        // first we insert the old record
        KeyAwareTransactionLogRecord oldRecord = mock(KeyAwareTransactionLogRecord.class);
        when(oldRecord.getKey()).thenReturn(key);
        log.add(oldRecord);

        // then we insert the old record
        KeyAwareTransactionLogRecord newRecord = mock(KeyAwareTransactionLogRecord.class);
        when(newRecord.getKey()).thenReturn(key);
        log.add(newRecord);

        assertSame(newRecord, log.get(key));
        assertEquals(1, log.size());
    }

    @Test
    public void remove_whenNotExist_thenCallIgnored() {
        TransactionLog log = new TransactionLog();
        log.remove("not exist");
    }

    @Test
    public void remove_whenExist_thenRemoved() {
        TransactionLog log = new TransactionLog();
        KeyAwareTransactionLogRecord record = mock(KeyAwareTransactionLogRecord.class);
        String key = "foo";
        when(record.getKey()).thenReturn(key);
        log.add(record);

        log.remove(key);

        assertNull(log.get(key));
    }
}
