/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.AbstractRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.OwnedEntryCostEstimatorFactory.createMapSizeEstimator;

/**
 * Default implementation of {@link Storage} layer used by a {@link RecordStore}
 *
 * @param <R> the value type to be put in this storage.
 */
public class StorageImpl<R extends Record> implements Storage<Data, R> {

    private final RecordFactory<R> recordFactory;
    private final StorageSCHM<R> records;

    // not final for testing purposes.
    private EntryCostEstimator<Data, Record> entryCostEstimator;

    StorageImpl(RecordFactory<R> recordFactory, InMemoryFormat inMemoryFormat, SerializationService serializationService) {
        this.recordFactory = recordFactory;
        this.entryCostEstimator = createMapSizeEstimator(inMemoryFormat);
        this.records = new StorageSCHM<R>(serializationService);
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        records.clear();

        entryCostEstimator.reset();
    }

    @Override
    public Collection<R> values() {
        return records.values();
    }

    @Override
    public Iterator<R> mutationTolerantIterator() {
        return records.values().iterator();
    }

    @Override
    public void put(Data key, R record) {

        ((AbstractRecord) record).setKey(key);

        R previousRecord = records.put(key, record);

        if (previousRecord == null) {
            updateCostEstimate(entryCostEstimator.calculateEntryCost(key, record));
        } else {
            updateCostEstimate(-entryCostEstimator.calculateValueCost(previousRecord));
            updateCostEstimate(entryCostEstimator.calculateValueCost(record));
        }
    }

    @Override
    public void updateRecordValue(Data key, R record, Object value) {
        updateCostEstimate(-entryCostEstimator.calculateValueCost(record));

        recordFactory.setValue(record, value);

        updateCostEstimate(entryCostEstimator.calculateValueCost(record));
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
    public void removeRecord(R record) {
        if (record == null) {
            return;
        }

        Data key = record.getKey();
        records.remove(key);

        updateCostEstimate(-entryCostEstimator.calculateEntryCost(key, record));
    }

    protected void updateCostEstimate(long entrySize) {
        entryCostEstimator.adjustEstimateBy(entrySize);
    }

    public void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        this.entryCostEstimator = entryCostEstimator;
    }

    @Override
    public void disposeDeferredBlocks() {
        // NOP intentionally.
    }

    @Override
    public Iterable<LazyEntryViewFromRecord> getRandomSamples(int sampleCount) {
        return records.getRandomSamples(sampleCount);
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        List<Data> keys = new ArrayList<Data>(size);
        int newTableIndex = records.fetchKeys(tableIndex, size, keys);
        return new MapKeysWithCursor(keys, newTableIndex);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size, SerializationService serializationService) {
        List<Map.Entry<Data, R>> entries = new ArrayList<Map.Entry<Data, R>>(size);
        int newTableIndex = records.fetchEntries(tableIndex, size, entries);
        List<Map.Entry<Data, Data>> entriesData = new ArrayList<Map.Entry<Data, Data>>(entries.size());
        for (Map.Entry<Data, R> entry : entries) {
            R record = entry.getValue();
            Data dataValue = serializationService.toData(record.getValue());
            entriesData.add(new AbstractMap.SimpleEntry<Data, Data>(entry.getKey(), dataValue));
        }
        return new MapEntriesWithCursor(entriesData, newTableIndex);
    }

}
