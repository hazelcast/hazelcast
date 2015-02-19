package com.hazelcast.cache.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    @Test
    public void putAndGetRecordFromNearCacheObjectRecordStore() {
        putAndGetRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndGetRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndGetRecord(InMemoryFormat.BINARY);
    }

    @Test
    public void putAndRemoveRecordFromNearCacheObjectRecordStore() {
        putAndRemoveRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndRemoveRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndRemoveRecord(InMemoryFormat.BINARY);
    }

    @Test
    public void clearRecordsFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, false);
    }

    @Test
    public void clearRecordsSuccessfullyFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, false);
    }

    @Test(expected = IllegalStateException.class)
    public void destoryStoreFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, true);
    }

    @Test(expected = IllegalStateException.class)
    public void destoryStoreFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, true);
    }

    @Test
    public void statsCalculatedOnNearCacheObjectRecordStore() {
        statsCalculated(InMemoryFormat.OBJECT);
    }

    @Test
    public void statsCalculatedOnNearCacheDataRecordStore() {
        statsCalculated(InMemoryFormat.BINARY);
    }

    @Test
    public void ttlEvaluatedOnNearCacheObjectRecordStore() {
        ttlEvaluated(InMemoryFormat.OBJECT);
    }

    @Test
    public void ttlEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        ttlEvaluated(InMemoryFormat.BINARY);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheObjectRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.OBJECT);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.BINARY);
    }

}
