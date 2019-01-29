/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Puts records to map which are loaded from map store by {@link com.hazelcast.core.IMap#loadAll}
 */
public class PutFromLoadAllOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation,
        BackupAwareOperation {

    private List<Data> keyValueSequence;
    private List<Data> invalidationKeys;

    public PutFromLoadAllOperation() {
        keyValueSequence = Collections.emptyList();
    }

    public PutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        super(name);
        checkFalse(isEmpty(keyValueSequence), "key-value sequence cannot be empty or null");
        this.keyValueSequence = keyValueSequence;
    }

    @Override
    public void run() throws Exception {
        boolean hasInterceptor = mapServiceContext.hasInterceptor(name);

        List<Data> keyValueSequence = this.keyValueSequence;
        for (int i = 0; i < keyValueSequence.size(); i += 2) {
            Data key = keyValueSequence.get(i);
            Data dataValue = keyValueSequence.get(i + 1);

            checkNotNull(key, "Key loaded by a MapLoader cannot be null.");

            // here object conversion is for interceptors.
            Object value = hasInterceptor ? mapServiceContext.toObject(dataValue) : dataValue;
            Object previousValue = recordStore.putFromLoad(key, value);
            // the following check is for the case when the putFromLoad does not put the data due to various reasons
            // one of the reasons may be size eviction threshold has been reached
            if (value != null && !recordStore.existInMemory(key)) {
                continue;
            }

            // do not run interceptors in case the put was skipped due to null value
            if (value != null) {
                callAfterPutInterceptors(value);
            }
            Record record = recordStore.getRecord(key);
            if (isPostProcessing(recordStore)) {
                checkNotNull(record, "Value loaded by a MapLoader cannot be null.");
                value = record.getValue();
            }
            publishEntryEvent(key, previousValue, value);
            publishWanReplicationEvent(key, value, record);
            addInvalidation(key);
        }
    }

    private void addInvalidation(Data key) {
        if (!mapContainer.hasInvalidationListener()) {
            return;
        }

        if (invalidationKeys == null) {
            invalidationKeys = new ArrayList<Data>(keyValueSequence.size() / 2);
        }

        invalidationKeys.add(key);
    }


    private void callAfterPutInterceptors(Object value) {
        mapService.getMapServiceContext().interceptAfterPut(name, value);
    }

    private void publishEntryEvent(Data key, Object previousValue, Object newValue) {
        final EntryEventType eventType = previousValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
        mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, key, previousValue, newValue);
    }

    private void publishWanReplicationEvent(Data key, Object value, Record record) {
        if (record == null || !mapContainer.isWanReplicationEnabled()) {
            return;
        }

        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        value = mapServiceContext.toData(value);
        EntryView entryView = EntryViews.createSimpleEntryView(key, value, record);
        mapEventPublisher.publishWanReplicationUpdate(name, entryView);
    }

    @Override
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);
        evict(null);

        super.afterRun();
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return !keyValueSequence.isEmpty();
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutFromLoadAllBackupOperation(name, keyValueSequence);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final List<Data> keyValueSequence = this.keyValueSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            out.writeData(data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size < 1) {
            keyValueSequence = Collections.emptyList();
        } else {
            final List<Data> tmpKeyValueSequence = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                final Data data = in.readData();
                tmpKeyValueSequence.add(data);
            }
            keyValueSequence = tmpKeyValueSequence;
        }
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_FROM_LOAD_ALL;
    }
}
