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

import com.hazelcast.cache.record.CacheRecord;
import com.hazelcast.cache.record.CacheRecordFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.CacheConcurrentHashMap;
import com.hazelcast.util.Clock;

import javax.cache.configuration.Factory;
import javax.cache.event.EventType;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.record.CacheRecordFactory.isExpiredAt;

public class CacheRecordStore implements ICacheRecordStore {

    final String name;
    final int partitionId;
    final NodeEngine nodeEngine;
    final CacheService cacheService;
    final CacheStatistics statistics;
    final CacheConfig cacheConfig;

    final CacheConcurrentHashMap<Data, CacheRecord> records = new CacheConcurrentHashMap<Data, CacheRecord>(1000);

    final CacheRecordFactory cacheRecordFactory;
    final ScheduledFuture<?> evictionTaskFuture;

    private CacheLoader cacheLoader;
    private CacheWriter cacheWriter;

    private boolean hasExpiringEntry = false;
    private boolean isEventsEnabled = true;

    private ExpiryPolicy defaultExpiryPolicy;

    CacheRecordStore(String name, int partitionId, NodeEngine nodeEngine, final CacheService cacheService) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(name);
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            final Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
            cacheWriter =  cacheWriterFactory.create();
        }
        evictionTaskFuture = nodeEngine.getExecutionService()
                .scheduleWithFixedDelay("hz:cache", new EvictionTask(), 5, 5, TimeUnit.SECONDS);
        this.statistics = new CacheStatistics();
        this.cacheRecordFactory = new CacheRecordFactory(cacheConfig.getInMemoryFormat(), nodeEngine.getSerializationService());
        final Factory<ExpiryPolicy> expiryPolicyFactory = cacheConfig.getExpiryPolicyFactory();
        defaultExpiryPolicy = expiryPolicyFactory.create();
    }


    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        long now = Clock.currentTimeMillis();

        Object value;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }

        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, record, _expiryPolicy, now, false);

            return value;
        } else {
            value = record.getValue();
            final long et;
            try {
                Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                if (expiryDuration != null) {
                    et = expiryDuration.getAdjustedTime(now);
                    record.setExpirationTime(et);
                }
            } catch (Throwable t) {
                //leave the expiry time untouched when we can't determine a duration
            }
            //TODO check this
//            if(isExpiredAt(et, now)){
//                processExpiredEntry(key, record);
//                if (isStatisticsEnabled()) {
//                    statistics.increaseCacheMisses(1);
//                }
//                return null;
//            }
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            return value;
        }
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        getAndPut(key, value, expiryPolicy, caller, false,false);
    }

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller, boolean getValue, boolean disableWriteThrough) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isPutSucceed;
        Object oldValue = null;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }

        //RI-COMMENT
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.

        if (record == null || isExpired) {
            isPutSucceed = createRecordWithExpiry(key, value, record, _expiryPolicy, now, disableWriteThrough);

        } else {
            oldValue = record.getValue();
            isPutSucceed = updateRecordWithExpiry(key, value, record, _expiryPolicy, now, disableWriteThrough);
        }
        if (isStatisticsEnabled()) {
            if (!getValue) {
                if (isPutSucceed) {
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNano(System.nanoTime() - start);
                }
            } else {
                if(isPutSucceed){
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNano(System.nanoTime() - start);
                }
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

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller, boolean getValue) {
        return getAndPut(key,value,expiryPolicy,caller,getValue,false);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return getAndPut(key,value,expiryPolicy,caller,true,false);
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return putIfAbsent(key,value,expiryPolicy,caller,false);
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller, boolean disableWriteThrough) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }
        if (record == null || isExpired) {
            result = createRecordWithExpiry(key, value, record, _expiryPolicy, now, disableWriteThrough);

        } else {
            result = false;
        }
        if (result && isStatisticsEnabled()) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    @Override
    public Object getAndRemove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        Object result = null;
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            deleteRecord(key);
        }
        if (isStatisticsEnabled()) {
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
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        if (record == null || isExpired) {
            return false;
        } else {
            deleteRecord(key);
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return true;
    }

    @Override
    public boolean remove(Data key, Object value, String caller) {
        final ExpiryPolicy _expiryPolicy = defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        int hitCount = 0;

        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            return false;
        } else {
            hitCount++;
            if (compare(record.getValue(), value)) {
                deleteCacheEntry(key);
                deleteRecord(key);
            } else {
                long et = -1l;
                try {
                    Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                    if (expiryDuration != null) {
                        et = expiryDuration.getAdjustedTime(now);
                        record.setExpirationTime(et);
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
        if (isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }

        }
        return true;
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, _expiryPolicy, now, false);
        }
        if (isStatisticsEnabled()) {
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
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isHit = false;
        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = false;
        } else {
            isHit = true;
            Object value = record.getValue();

            if (compare(value, oldValue)) {
                result = updateRecordWithExpiry(key, newValue, record, _expiryPolicy, now, false);
            } else {
                try {
                    Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
                    if (expiryDuration != null) {
                        long et = expiryDuration.getAdjustedTime(now);
                        record.setExpirationTime(et);
                    }
                } catch (Throwable t) {
                    //leave the expiry time untouched when we can't determine a duration
                }
                result = false;
            }

        }
        if (isStatisticsEnabled()) {
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
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        Object result = null;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            updateRecordWithExpiry(key, value, record, _expiryPolicy, now, false);
        }
        if (isStatisticsEnabled()) {
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
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (isExpired) {
            processExpiredEntry(key, record);
        }
        return record != null && !isExpired;
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        //we don not call loadAll. shouldn't we ?
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
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
            final Set<Data> _keys = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys );
            try {
                final int initialCount = _keys.size();
                deleteAllCacheEntry(_keys);
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheRemovals(initialCount - _keys.size());
                }
            } finally {
                final Set<Data> _keysToClean = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys );
                for (Data key : _keysToClean) {
                    if (_keys.contains(key) && records.containsKey(key) ){
                        deleteRecord(key);
                    } else {
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

    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    public void setRecord(Data key, CacheRecord record) {
        if (key != null && record != null) {
            records.put(key, record);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        if (key != null) {
            return records.remove(key);
        }
        return null;
    }

    @Override
    public CacheKeyIteratorResult iterator(int tableIndex, int size) {
        return records.fetchNext( tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (isExpired) {
            processExpiredEntry(key, record);
            record = null;
        }
        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = new CacheEntryProcessorEntry(key, record, this, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
    }

    @Override
    public Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues) {
        Set<Data> keysLoadad = new HashSet<Data>();
        Map<Data,Object> loaded = loadAllCacheEntry(keys);
        if(loaded != null && !loaded.isEmpty()){
            if(replaceExistingValues){
                for(Map.Entry<Data,Object> entry:loaded.entrySet()){
                    final Data key = entry.getKey();
                    final Object value = entry.getValue();
                    if(value != null){
                        getAndPut(key, value, null, null, false,false);
//                        this.disableWriteThrough = false;
//                        put(key, value, null, null);
//                        this.disableWriteThrough = true;//cacheConfig.isWriteThrough();
                        keysLoadad.add(key);
                    }
                }
            } else {
                for(Map.Entry<Data,Object> entry:loaded.entrySet()){
                    final Data key = entry.getKey();
                    final Object value = entry.getValue();
                    if(value != null){
//                        this.disableWriteThrough = false;
                        final boolean hasPut = putIfAbsent(key, value, null, null,false);
//                        this.disableWriteThrough = true;//cacheConfig.isWriteThrough();
                        if(hasPut){
                            keysLoadad.add(key);
                        }
                    }
                }
            }
        }
        return keysLoadad;
    }

    @Override
    public CacheStatistics getCacheStats() {
        return statistics;
    }

    public CacheConfig getConfig() {
        return cacheConfig;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<Data, CacheRecord> getReadOnlyRecords() {
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

    boolean createRecordWithExpiry(Data key, Object value, CacheRecord record, ExpiryPolicy _expiryPolicy, long now,
                                   boolean disableWriteThrough) {
        Duration expiryDuration;
        try {
            expiryDuration = _expiryPolicy.getExpiryForCreation();
        } catch (Throwable t) {
            expiryDuration = Duration.ETERNAL;
        }
        long et = expiryDuration.getAdjustedTime(now);

        if(!disableWriteThrough){
            writeThroughCache(key, value);
        }
        record = createRecord(key, value, et);

        if (isExpiredAt(et, now)) {
            processExpiredEntry(key, record);
        } else {
            records.put(key, record);
            return true;
        }
        return false;
    }

    private CacheRecord createRecord(Data keyData, Object value, long expirationTime) {
        final CacheRecord record = cacheRecordFactory.newRecordWithExpiry(keyData, value, expirationTime);

        if (isEventsEnabled) {
            final Object recordValue = record.getValue();

            Data dataValue;
            if (!(recordValue instanceof Data)) {
                dataValue = cacheService.toData(recordValue);
            } else {
                dataValue = (Data) recordValue;
            }
            cacheService.publishEvent(name, EventType.CREATED, record.getKey(), null, dataValue);
        }
        return record;
    }

    boolean updateRecordWithExpiry(Data key, Object value, CacheRecord record, ExpiryPolicy _expiryPolicy, long now,
                                   boolean disableWriteThrough) {
        long et = -1l;
        try {
            Duration expiryDuration = _expiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                et = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(et);
            }
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }
        if(!disableWriteThrough){
            writeThroughCache(key, value);
        }
        updateRecord(record, value);

        if (isExpiredAt(et, now)) {
            processExpiredEntry(key, record);
        } else {
            return true;
        }
        return false;
    }

    private CacheRecord updateRecord(CacheRecord record, Object value) {
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
        if (isEventsEnabled) {
            cacheService.publishEvent(name, EventType.UPDATED, record.getKey(), dataOldValue, dataValue);
        }
        return record;
    }

    void deleteRecord(Data key) {
        final CacheRecord record = records.remove(key);
        if (isEventsEnabled) {
            cacheService.publishEvent(name, EventType.REMOVED, record.getKey(), null,  record.getValue());
        }
    }

    CacheRecord accessRecord(CacheRecord record, ExpiryPolicy expiryPolicy, long now) {
        final ExpiryPolicy _expiryPolicy = expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
        final long et;
        try {
            Duration expiryDuration = _expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                et = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(et);
            }
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }
        return record;
    }

    CacheRecord readThroughRecord(Data key, long now) {
        final ExpiryPolicy _expiryPolicy = defaultExpiryPolicy;
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
        final CacheRecord record = createRecord(key, value, et);
        return record;
    }

    protected Object readThroughCache(Data key) throws CacheLoaderException {
        if (this.isReadThrough() && cacheLoader != null) {
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
        if (isWriteThrough() && cacheWriter != null) {
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
        if (isWriteThrough() && cacheWriter != null) {
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
     *
     * @param keys : keys to delete, after method returns it includes only deleted keys
     */
    void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough() && cacheWriter != null && keys != null && !keys.isEmpty()) {
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

    Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
        if (cacheLoader != null) {
            Map<Object, Data> keysToLoad = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object _keyObj = cacheService.toObject(key);
                keysToLoad.put(_keyObj, key);
            }

            Map<Object, Object> loaded;
            try {
                loaded = cacheLoader.loadAll(keysToLoad.keySet());
            } catch (Throwable e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during loadAll", e);
                } else {
                    throw (CacheLoaderException)e;
                }
            }
            Map<Data,Object> result= new HashMap<Data, Object>();
            for(Object keyObj:keysToLoad.keySet()){
                final Object valueObject = loaded.get(keyObj);
                final Data key = keysToLoad.get(keyObj);
                result.put(key,valueObject);
            }
            return result;
        }
        return null;
    }

    void processExpiredEntry(Data key, CacheRecord record) {
        records.remove(key);
        if (isEventsEnabled) {
            cacheService.publishEvent(name, EventType.EXPIRED, key, record.getValue(), null);
        }
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

    public boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    public boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    public boolean isEventsEnabled() {
        return isEventsEnabled;
    }

    public boolean isStatisticsEnabled() {
        return cacheConfig.isStatisticsEnabled();
    }
}
