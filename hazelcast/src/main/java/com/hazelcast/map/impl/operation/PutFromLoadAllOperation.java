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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Puts records to map which are loaded from map store by {@link IMap#loadAll}
 */
public class PutFromLoadAllOperation extends MapOperation
        implements PartitionAwareOperation, MutatingOperation, BackupAwareOperation {

    private List<Data> loadingSequence;
    private List<Data> invalidationKeys;
    private boolean includesExpirationTime;

    public PutFromLoadAllOperation() {
        loadingSequence = Collections.emptyList();
        includesExpirationTime = false;
    }

    public PutFromLoadAllOperation(String name, List<Data> loadingSequence, boolean includesExpirationTime) {
        super(name);
        checkFalse(isEmpty(loadingSequence), "key-value sequence cannot be empty or null");
        this.loadingSequence = loadingSequence;
        this.includesExpirationTime = includesExpirationTime;
    }

    @Override
    protected void runInternal() {
        boolean hasInterceptor = !mapContainer.getInterceptorRegistry()
                .getInterceptors().isEmpty();

        List<Data> loadingSequence = this.loadingSequence;
        for (int i = 0; i < loadingSequence.size(); ) {
            Data key = loadingSequence.get(i++);
            Data dataValue = loadingSequence.get(i++);

            checkNotNull(key, "Key loaded by a MapLoader cannot be null.");

            // here object conversion is for interceptors.
            Object value = hasInterceptor ? mapServiceContext.toObject(dataValue) : dataValue;

            if (includesExpirationTime) {
                long expirationTime = (long) mapServiceContext.toObject(loadingSequence.get(i++));
                recordStore.putFromLoad(key, value, expirationTime, getCallerAddress());
            } else {
                recordStore.putFromLoad(key, value, getCallerAddress());
            }
            // the following check is for the case when the putFromLoad does not put
            // the data due to various reasons one of the reasons may be size
            // eviction threshold has been reached
            if (value != null && !recordStore.existInMemory(key)) {
                continue;
            }

            // do not run interceptors in case the put was skipped due to null value
            if (value != null) {
                callAfterPutInterceptors(value);
            }

            if (isPostProcessing(recordStore)) {
                Record record = recordStore.getRecord(key);
                checkNotNull(record, "Value loaded by a MapLoader cannot be null.");
                value = record.getValue();
            }
            publishLoadAsWanUpdate(key, value);
            addInvalidation(key);
        }
    }

    private void addInvalidation(Data key) {
        if (!mapContainer.hasInvalidationListener()) {
            return;
        }

        if (invalidationKeys == null) {
            if (includesExpirationTime) {
                invalidationKeys = new ArrayList<>(loadingSequence.size() / 3);
            } else {
                invalidationKeys = new ArrayList<>(loadingSequence.size() / 2);
            }
        }

        invalidationKeys.add(key);
    }

    private void callAfterPutInterceptors(Object value) {
        mapService.getMapServiceContext()
                .interceptAfterPut(mapContainer.getInterceptorRegistry(), value);
    }

    @Override
    protected void afterRunInternal() {
        invalidateNearCache(invalidationKeys);
        evict(null);

        super.afterRunInternal();
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return !loadingSequence.isEmpty();
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
        return new PutFromLoadAllBackupOperation(name, loadingSequence, includesExpirationTime);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(includesExpirationTime);
        final List<Data> keyValueSequence = this.loadingSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.includesExpirationTime = in.readBoolean();
        final int size = in.readInt();
        if (size < 1) {
            loadingSequence = Collections.emptyList();
        } else {
            final List<Data> tmpKeyValueSequence = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final Data data = IOUtil.readData(in);
                tmpKeyValueSequence.add(data);
            }
            loadingSequence = tmpKeyValueSequence;
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_FROM_LOAD_ALL;
    }
}
