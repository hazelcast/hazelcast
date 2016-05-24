/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.record.AbstractRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;

import static com.hazelcast.map.impl.SizeEstimators.createMapSizeEstimator;

/**
 * Default implementation of {@link Storage} layer used by a {@link RecordStore}
 *
 * @param <R> the value type to be put in this storage.
 */
public class StorageImpl<R extends Record> implements Storage<Data, R> {

    private final RecordFactory<R> recordFactory;
    private final StorageSCHM<R> records;

    // not final for testing purposes.
    private SizeEstimator sizeEstimator;

    StorageImpl(RecordFactory<R> recordFactory, InMemoryFormat inMemoryFormat, SerializationService serializationService) {
        this.recordFactory = recordFactory;
        this.sizeEstimator = createMapSizeEstimator(inMemoryFormat);
        this.records = new StorageSCHM<R>(serializationService);
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        records.clear();

        sizeEstimator.reset();
    }

    @Override
    public Collection<R> values() {
        return records.values();
    }

    @Override
    public void put(Data key, R record) {

        ((AbstractRecord) record).setKey(key);

        R previousRecord = records.put(key, record);

        if (previousRecord == null) {
            updateSizeEstimator(calculateHeapCost(key));
        }

        updateSizeEstimator(-calculateHeapCost(previousRecord));
        updateSizeEstimator(calculateHeapCost(record));
    }

    @Override
    public void updateRecordValue(Data key, R record, Object value) {
        updateSizeEstimator(-calculateHeapCost(record));

        recordFactory.setValue(record, value);

        updateSizeEstimator(calculateHeapCost(record));
    }

    @Override
    public R get(Data key) {
        return records.get(key);
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

    @Override
    public SizeEstimator getSizeEstimator() {
        return sizeEstimator;
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

        updateSizeEstimator(-calculateHeapCost(record));
        updateSizeEstimator(-calculateHeapCost(key));
    }

    protected void updateSizeEstimator(long recordSize) {
        sizeEstimator.add(recordSize);
    }

    protected long calculateHeapCost(Object obj) {
        return sizeEstimator.calculateSize(obj);
    }

    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        this.sizeEstimator = sizeEstimator;
    }

    @Override
    public void disposeDeferredBlocks() {
        // NOP intentionally.
    }

    @Override
    public Iterable<LazyEntryViewFromRecord> getRandomSamples(int sampleCount) {
        return records.getRandomSamples(sampleCount);
    }

}
