/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.internal.util.IterableUtil.asReadOnlyIterator;
import static com.hazelcast.map.impl.OwnedEntryCostEstimatorFactory.createMapSizeEstimator;

/**
 * Default implementation of {@link Storage} layer used by a {@link RecordStore}
 *
 * @param <R> the value type to be put in this storage.
 */
public class StorageImpl<R extends Record> implements Storage<Data, R> {

    private final StorageSCHM<R> records;
    private final SerializationService serializationService;
    private final InMemoryFormat inMemoryFormat;

    // not final for testing purposes.
    private EntryCostEstimator<Data, Record> entryCostEstimator;

    StorageImpl(InMemoryFormat inMemoryFormat, ExpirySystem expirySystem,
                SerializationService serializationService) {
        this.entryCostEstimator = createMapSizeEstimator(inMemoryFormat);
        this.inMemoryFormat = inMemoryFormat;
        this.records = new StorageSCHM<>(serializationService, expirySystem);
        this.serializationService = serializationService;
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        records.clear();

        entryCostEstimator.reset();
    }

    @Override
    public Iterator<Map.Entry<Data, R>> mutationTolerantIterator() {
        return asReadOnlyIterator(records.cachedEntrySet().iterator());
    }

    @Override
    public void put(Data key, R record) {
        R previousRecord = records.put(key, record);

        if (previousRecord == null) {
            updateCostEstimate(entryCostEstimator.calculateEntryCost(key, record));
        } else {
            updateCostEstimate(-entryCostEstimator.calculateValueCost(previousRecord));
            updateCostEstimate(entryCostEstimator.calculateValueCost(record));
        }
    }

    @Override
    public R updateRecordValue(Data key, R record, Object value) {
        updateCostEstimate(-entryCostEstimator.calculateValueCost(record));

        record.setValue(inMemoryFormat == BINARY
                ? serializationService.toData(value) : serializationService.toObject(value));

        updateCostEstimate(entryCostEstimator.calculateValueCost(record));
        return record;
    }

    @Override
    public R get(Data key) {
        return records.get(key);
    }

    @Override
    public R getIfSameKey(Data key) {
        throw new UnsupportedOperationException("StorageImpl#getIfSameKey");
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public void destroy(boolean isDuringShutdown) {
        clear(isDuringShutdown);
    }

    public EntryCostEstimator getEntryCostEstimator() {
        return entryCostEstimator;
    }

    @Override
    public boolean containsKey(Data key) {
        return records.containsKey(key);
    }

    @Override
    public void removeRecord(Data dataKey, R record) {
        records.remove(dataKey);

        updateCostEstimate(-entryCostEstimator.calculateEntryCost(dataKey, record));
    }

    protected void updateCostEstimate(long entrySize) {
        entryCostEstimator.adjustEstimateBy(entrySize);
    }

    public void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        this.entryCostEstimator = entryCostEstimator;
    }

    @Override
    public Iterable getRandomSamples(int sampleCount) {
        return records.getRandomSamples(sampleCount);
    }

    @Override
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        List<Data> keys = new ArrayList<>(size);
        IterationPointer[] newPointers = records.fetchKeys(pointers, size, keys);
        return new MapKeysWithCursor(keys, newPointers);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        List<Map.Entry<Data, R>> entries = new ArrayList<>(size);
        IterationPointer[] newPointers = records.fetchEntries(pointers, size, entries);
        List<Map.Entry<Data, Data>> entriesData = new ArrayList<>(entries.size());
        for (Map.Entry<Data, R> entry : entries) {
            R record = entry.getValue();
            Data dataValue = serializationService.toData(record.getValue());
            entriesData.add(new AbstractMap.SimpleEntry<>(entry.getKey(), dataValue));
        }
        return new MapEntriesWithCursor(entriesData, newPointers);
    }

    @Override
    public Data extractDataKeyFromLazy(EntryView entryView) {
        return ((LazyEvictableEntryView) entryView).getDataKey();
    }

    @Override
    public Data toBackingDataKeyFormat(Data key) {
        return key;
    }
}
