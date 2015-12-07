package com.hazelcast.map.impl;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.nio.serialization.Data;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by nangel on 3/12/15.
 */
public class DefaultRecordStoreTest {

    @Test
    public void testReadBackupWhenReturnIsNull() throws Exception {
        DefaultRecordStore defaultRecordStore = mock(DefaultRecordStore.class);
        when(defaultRecordStore.getRecord(mock(Data.class))).thenReturn(null);
        Object o = defaultRecordStore.readBackup(mock(Data.class));
        assertNull(o);
    }

    @Test
    public void testReadBackupWhenReturnIsExpired() throws Exception {
        DefaultRecordStore defaultRecordStore = mock(DefaultRecordStore.class);
        long now = new Date(9999).getTime();
        Record record = mock(Record.class);
        doReturn(record).when(defaultRecordStore).getRecord(mock(Data.class));
        doReturn(true).when(defaultRecordStore).isExpired(null, now, false);
        Object o = defaultRecordStore.readBackup(mock(Data.class));
        assertNull(o);
    }

    @Test
    public void testReadBackupDataWhenObjectIsNull() throws Exception {
        DefaultRecordStore defaultRecordStore = mock(DefaultRecordStore.class);
        doReturn(null).when(defaultRecordStore).readBackup(any(Data.class));
        Data data = defaultRecordStore.readBackupData(mock(Data.class));
        assertNull(data);
    }
}