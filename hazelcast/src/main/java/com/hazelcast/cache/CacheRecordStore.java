/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.record.DataRecord;
import com.hazelcast.map.record.ObjectRecord;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordStatistics;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.CacheConcurrentHashMap;
import com.hazelcast.util.Clock;

import javax.cache.event.EventType;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.record.RecordStatistics.isExpiredAt;

public class CacheRecordStore implements ICacheRecordStore {

    final String name;
    final int partitionId;
    final NodeEngine nodeEngine;
    final CacheService cacheService;
    final CacheStatistics statistics;
    final CacheConfig<Object, Object> cacheConfig;

    final CacheConcurrentHashMap<Data, Record> records = new CacheConcurrentHashMap<Data, Record>(1000);

    final ScheduledFuture<?> evictionTaskFuture;

    private CacheLoader<Object, Object> cacheLoader;
    private CacheWriter<? super Object, ? super Object> cacheWriter;

    private boolean hasExpiringEntry = false;
    private boolean isStatisticsEnabled = false;
    private boolean isEventsEnabled = false;
    private boolean isWriteThrough = false;
    private boolean isReadThrough = false;

    CacheRecordStore(final String name, int partitionId, NodeEngine nodeEngine, final CacheService cacheService) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;

        cacheConfig = nodeEngine.getConfig().findCacheConfig(name);

        if (cacheConfig.getCacheLoaderFactory() != null) {
            cacheLoader = cacheConfig.getCacheLoaderFactory().create();
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            cacheWriter = cacheConfig.getCacheWriterFactory().create();
        }
        evictionTaskFuture = nodeEngine.getExecutionService()
                .scheduleWithFixedDelay("hz:cache", new EvictionTask(), 5, 5, TimeUnit.SECONDS);

        this.statistics = new CacheStatistics();

        this.isStatisticsEnabled = cacheConfig.isStatisticsEnabled();

        this.isWriteThrough = cacheConfig.isWriteThrough();
        this.isReadThrough = cacheConfig.isReadThrough();
    }


    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        long now = Clock.currentTimeMillis();

        Object value;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }

        if (record == null || isExpired) {
            if (isStatisticsEnabled) {
                statistics.increaseCacheMisses(1);
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, record, _expiryPolicy, now);

            return value;
        } else {
            value = record.getValue();
            final long et;
            try {
                Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                if (expiryDuration != null) {
                    et = expiryDuration.getAdjustedTime(now);
                    record.getStatistics().setExpirationTime(et);
                }
            } catch (Throwable t) {
                //leave the expiry time untouched when we can't determine a duration
            }
            //TODO check this
//            if(isExpiredAt(et, now)){
//                processExpiredEntry(key, record);
//                if (isStatisticsEnabled) {
//                    statistics.increaseCacheMisses(1);
//                }
//                return null;
//            }
            if (isStatisticsEnabled) {
                statistics.increaseCacheHits(1);
            }
            return value;
        }
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        getAndPut(key, value, expiryPolicy, caller, false);
    }

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller, boolean getValue) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        boolean isPutSucceed;
        Object oldValue = null;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }

        //RI-COMMENT
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.

        if (record == null || isExpired) {
            isPutSucceed = createRecordWithExpiry(key, value, record, _expiryPolicy, now);

        } else {
            oldValue = record.getValue();
            isPutSucceed = updateRecordWithExpiry(key, value, record, _expiryPolicy, now);
        }
        if (isStatisticsEnabled) {
            if (!getValue) {
                if (isPutSucceed) {
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNano(System.nanoTime() - start);
                }
            } else {
                if (oldValue == null) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNano(System.nanoTime() - start);
            }
        }
        return oldValue;
    }


    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return getAndPut(key, value, expiryPolicy, caller, true);
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        boolean result;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }
        if (record == null || isExpired) {
            result = createRecordWithExpiry(key, value, record, _expiryPolicy, now);

        } else {
            result = false;
        }
        if (result && isStatisticsEnabled) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    @Override
    public Object getAndRemove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);

        Object result = null;
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            deleteRecord(key);
        }
        if (isStatisticsEnabled) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public boolean remove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);

        if (record == null || isExpired) {
            return false;
        } else {
            deleteRecord(key);
        }
        if (isStatisticsEnabled) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return true;
    }

    @Override
    public boolean remove(Data key, Object value, String caller) {
        final ExpiryPolicy _expiryPolicy = cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);

        if (record == null || isExpired) {
            return false;
        } else {
            if (compare(record.getValue(), value)) {
                deleteCacheEntry(key);
                deleteRecord(key);
            } else {
                long et = -1l;
                try {
                    Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                    if (expiryDuration != null) {
                        et = expiryDuration.getAdjustedTime(now);
                        record.getStatistics().setExpirationTime(et);
                    }
                } catch (Throwable t) {
                    //leave the expiry time untouched when we can't determine a duration
                }
                if (isExpiredAt(et, now)) {
                    processExpiredEntry(key, record);
                }
                return false;
            }
        }
        if (isStatisticsEnabled) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return true;
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        boolean result;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (record == null || isExpired) {
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, _expiryPolicy, now);
        }
        if (isStatisticsEnabled) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.increaseCacheHits(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }

        return result;
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, String caller) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        boolean isHit = false;
        boolean result;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (record == null || isExpired) {
            result = false;
        } else {
            isHit = true;
            Object value = record.getValue();

            if (compare(value, oldValue)) {
                result = updateRecordWithExpiry(key, value, record, _expiryPolicy, now);
            } else {
                try {
                    Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                    if (expiryDuration != null) {
                        long et = expiryDuration.getAdjustedTime(now);
                        record.getStatistics().setExpirationTime(et);
                    }
                } catch (Throwable t) {
                    //leave the expiry time untouched when we can't determine a duration
                }
                result = false;
            }

        }
        if (isStatisticsEnabled) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (isHit) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        Object result = null;
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            updateRecordWithExpiry(key, value, record, _expiryPolicy, now);
        }
        if (isStatisticsEnabled) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }
        return record != null && !isExpired;
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        //we don not call loadAll. shouldn't we ?
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final MapEntrySet result = new MapEntrySet();

        for (Data key : keySet) {
            final Object value = get(key, _expiryPolicy);
            if (value != null) {
                result.add(key, cacheService.toData(value));
            }
        }
        return result;
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public void clear(Set<Data> keys, boolean isRemoveAll) {
        if (isRemoveAll) {
            final Set<Data> _keys = keys == null ? records.keySet() : keys;
            try {
                final int initialCount = _keys.size();
                deleteAllCacheEntry(_keys);
                if (isStatisticsEnabled) {
                    statistics.increaseCacheRemovals(initialCount - _keys.size());
                }
            } finally {
                final Set<Data> _keysToClean = keys == null ? records.keySet() : keys;
                for (Data key : _keysToClean) {
                    if (_keys.contains(key) && records.containsKey(key) ) {
                        deleteRecord(key);
                        keys.remove(key);
                    }
                }
            }
        } else {
            records.clear();
        }
    }

    @Override
    public void destroy() {
        clear(null, false);
        onDestroy();
    }

    public void onDestroy() {
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    public void own(Data key, Object value, RecordStatistics recordStatistics) {
        if (recordStatistics != null && !recordStatistics.isExpiredAt(Clock.currentTimeMillis())) {
            Record record = createRecord(key, value);
            record.setStatistics(recordStatistics);
            records.put(key, record);
        }
    }

    @Override
    public CacheKeyIteratorResult iterator(int segmentIndex, int tableIndex, int size) {
        return records.keySet(segmentIndex, tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled ? System.nanoTime() : 0;

        Record record = records.get(key);
        boolean isExpired = record != null && isExpiredAt(record.getStatistics().getExpirationTime(), now);
        if (isExpired) {
            processExpiredEntry(key, record);
            record = null;
        }
        if (record == null || isExpired) {
            if (isStatisticsEnabled) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
        }
        if (isStatisticsEnabled) {
            statistics.addGetTimeNano(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = new CacheEntryProcessorEntry(key, record, this, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
    }

    @Override
    public void setRecordStoreMode(boolean storeOrBackup) {
        if(storeOrBackup){ //STORE MODE
            this.isEventsEnabled=true;
            this.isEventsEnabled = cacheConfig.isStatisticsEnabled();
            this.isWriteThrough = cacheConfig.isWriteThrough();
            this.isReadThrough = cacheConfig.isReadThrough();
        } else {//BACKUP MODE
            this.isEventsEnabled=false;
            this.isEventsEnabled = false;
            this.isWriteThrough = false;
            this.isReadThrough = false;
        }
    }

    public CacheConfig getConfig() {
        return cacheConfig;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<Data, Record> getReadOnlyRecords() {
        return Collections.unmodifiableMap(records);
    }

    private class EvictionTask implements Runnable {

        public void run() {
            if (hasExpiringEntry) {

                evictExpiredRecords();
            }
        }
    }

    public void evictExpiredRecords() {

    }

    boolean createRecordWithExpiry(Data key, Object value, Record record, ExpiryPolicy _expiryPolicy, long now) {
        Duration expiryDuration;
        try {
            expiryDuration = _expiryPolicy.getExpiryForCreation();
        } catch (Throwable t) {
            expiryDuration = Duration.ETERNAL;
        }
        long et = expiryDuration.getAdjustedTime(now);

        if (isExpiredAt(et, now)) {
            processExpiredEntry(key, record);
        } else {
            writeThroughCache(key, value);
            record = createRecord(key, value);
            record.getStatistics().setExpirationTime(et);
            records.put(key, record);
            return true;
        }
        return false;
    }

    private Record createRecord(Data keyData, Object value) {
        Record record = null;
        final Data dataValue = cacheService.toData(value);
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                record = new DataRecord(keyData, dataValue);
                break;
            case OBJECT:
                record = new ObjectRecord(keyData, value);
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
        }
        if(isEventsEnabled){
            cacheService.publishEvent(name, EventType.CREATED, record.getKey(), null, dataValue);
        }
        return record;
    }

    boolean updateRecordWithExpiry(Data key, Object value, Record record, ExpiryPolicy _expiryPolicy, long now) {
        long et = -1l;
        try {
            Duration expiryDuration = _expiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                et = expiryDuration.getAdjustedTime(now);
                record.getStatistics().setExpirationTime(et);
            }
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }
        if (isExpiredAt(et, now)) {
            processExpiredEntry(key, record);
        } else {
            writeThroughCache(key, value);
            updateRecord(record, value);
            return true;
        }
        return false;
    }

    private Record updateRecord(Record record, Object value) {
        final Data dataOldValue;
        final Data dataValue;
        Object v = value;
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                if (!(value instanceof Data)) {
                    v = cacheService.toData(value);
                }
                dataValue = (Data) v;
                dataOldValue = (Data) record.getValue();
                break;
            case OBJECT:
                if (value instanceof Data) {
                    v = cacheService.toObject(value);
                }
                dataValue = (Data) value;
                dataOldValue = cacheService.toData(record.getValue());
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
        }
        record.setValue(v);
        if(isEventsEnabled){
            cacheService.publishEvent(name, EventType.UPDATED, record.getKey(), dataOldValue, dataValue);
        }
        return record;
    }

    void deleteRecord(Data key) {
        final Record record = records.remove(key);
        final Data dataOldValue;
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                dataOldValue = (Data) record.getValue();
                break;
            case OBJECT:
                dataOldValue = cacheService.toData(record.getValue());
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
        }
        if(isEventsEnabled){
            cacheService.publishEvent(name, EventType.REMOVED, record.getKey(), dataOldValue, null);
        }
    }

    Record accessRecord(Record record, ExpiryPolicy expiryPolicy, long now) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : cacheConfig.getExpiryPolicyFactory().create();
        final long et;
        try {
            Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                et = expiryDuration.getAdjustedTime(now);
                record.getStatistics().setExpirationTime(et);
            }
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }
        return record;
    }

    Record readThroughRecord(Data key, long now) {
        final ExpiryPolicy _expiryPolicy = cacheConfig.getExpiryPolicyFactory().create();
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration = null;
        try {
            expiryDuration = _expiryPolicy.getExpiryForCreation();
        } catch (Throwable t) {
            expiryDuration = Duration.ETERNAL;
        }
        long et = expiryDuration.getAdjustedTime(now);

        if (isExpiredAt(et, now)) {
            return null;
        }
        final Record record = createRecord(key, value);
        record.getStatistics().setExpirationTime(et);

        return record;
    }

    protected Object readThroughCache(Data key) throws CacheLoaderException {
        if (this.isReadThrough && cacheLoader != null) {
            try {
                Object o = cacheService.toObject(key);
                return cacheLoader.load(o);
            } catch (Exception e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during load", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
        }
        return null;
    }

    protected void writeThroughCache(Data key, Object value) throws CacheWriterException {
        if (cacheConfig.isWriteThrough() && cacheWriter != null) {
            try {
                final Object _key = cacheService.toObject(key);
                final Object _value;
                switch (cacheConfig.getInMemoryFormat()) {
                    case BINARY:
                        _value = cacheService.toObject(value);
                        break;
                    case OBJECT:
                        _value = value;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
                }
                CacheEntry<?, ?> entry = new CacheEntry<Object, Object>(_key, _value);
                cacheWriter.write(entry);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during write", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    void deleteCacheEntry(Data key) {
        if (isWriteThrough && cacheWriter != null) {
            try {
                final Object _key = cacheService.toObject(key);
                cacheWriter.delete(_key);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during delete", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    /**
     * modifies the keys
     * @param keys : keys to delete, after method returns it includes only deleted keys
     */
    void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough && cacheWriter != null) {
            if (keys != null && !keys.isEmpty()) {
                Map<Object, Data> keysToDelete = new HashMap<Object, Data>();
                for (Data key : keys) {
                    final Object _keyObj = cacheService.toObject(key);
                    keysToDelete.put(_keyObj, key);
                }
                final Set<Object> keysObject = keysToDelete.keySet();
                try {
                    cacheWriter.deleteAll(keysObject);
                } catch (Exception e) {
                    if (!(e instanceof CacheWriterException)) {
                        throw new CacheWriterException("Exception in CacheWriter during deleteAll", e);
                    } else {
                        throw (CacheWriterException) e;
                    }
                } finally {
                    for (Object undeletedKey : keysObject) {
                        final Data undeletedKeyData = keysToDelete.get(undeletedKey);
                        keys.remove(undeletedKeyData);
                    }
                }
            }
        }
    }

    void processExpiredEntry(Data key, Record record) {
        records.remove(key);
//        publishEvent(nodeEngine.getThisAddress(), name, EntryEventType.EVICTED,key, cacheService.toData(record.getValue()), null);
    }


    private boolean compare(Object v1, Object v2) {
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null) {
            return false;
        }
        if (v2 == null) {
            return false;
        }
        return v1.equals(v2);

    }


}
