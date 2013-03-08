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

package com.hazelcast.map.tx;

import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalOperation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 3/7/13
 */
public abstract class TransactionalMapOperation extends TransactionalOperation
        implements KeyBasedOperation, WaitSupport, BackupAwareOperation {

    protected String name;
    protected Data dataKey;
    protected int threadId = -1;
    protected Data dataValue = null;
    protected long ttl = -1;
    protected long timeout;

    protected transient MapService mapService;
    protected transient MapContainer mapContainer;
    protected transient PartitionContainer partitionContainer;
    protected transient RecordStore recordStore;

    protected TransactionalMapOperation() {
    }

    protected TransactionalMapOperation(String name, Data dataKey) {
        this.name = name;
        this.dataKey = dataKey;
    }

    protected TransactionalMapOperation(String name, Data dataKey, Data dataValue) {
        this.name = name;
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    public final void innerBeforeRun() throws Exception {
        mapService = getService();
        mapContainer = mapService.getMapContainer(name);
        partitionContainer = mapService.getPartitionContainer(getPartitionId());
        recordStore = partitionContainer.getRecordStore(name);
        if(!mapContainer.isMapReady()) {
            throw new RetryableHazelcastException("Map is not ready!!!");
        }
    }

    protected final void process() throws TransactionException {
        if (!recordStore.lock(getKey(), getCallerUuid(), threadId, timeout)) {
            throw new TransactionException("Cannot acquire lock!");
        }
        innerProcess();
    }

    protected abstract void innerProcess();

    protected final void onPrepare() throws TransactionException {
        if(!recordStore.extendLock(getKey(), getCallerUuid(), threadId, TimeUnit.MINUTES.toMillis(1))) {
            throw new TransactionException("Lock timed-out!");
        }
    }

    protected final void invalidateNearCaches() {
        final MapService mapService = getService();
        final MapContainer mapContainer = mapService.getMapContainer(name);
        if (mapContainer.isNearCacheEnabled()
                && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange()) {
            mapService.invalidateAllNearCaches(name, dataKey);
        }
    }

    protected final void onCommit() {
        try {
            innerOnCommit();
        } finally {
            recordStore.unlock(getKey(), getCallerUuid(), threadId);
        }
    }

    protected final void onRollback() {
        try {
            innerOnRollback();
        } finally {
            recordStore.unlock(getKey(), getCallerUuid(), threadId);
        }
    }

    protected abstract void innerOnCommit();

    protected abstract void innerOnRollback();

    public final String getName() {
        return name;
    }

    public final Data getKey() {
        return dataKey;
    }

    public final int getThreadId() {
        return threadId;
    }

    public final void setThreadId(int threadId) {
        this.threadId = threadId;
    }

    public final void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public final Data getValue() {
        return dataValue;
    }

    public final long getTtl() {
        return ttl;
    }

    public final int getKeyHash() {
        return dataKey != null ? dataKey.getPartitionHash() : 0;
    }

    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new LockNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    public final long getWaitTimeoutMillis() {
        return timeout;
    }

    public final void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(new TransactionException("Transaction timed-out!"));
    }

    public boolean shouldBackup() {
        return isCommitted();
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        dataKey.writeData(out);
        IOUtil.writeNullableData(out, dataValue);
        out.writeInt(threadId);
        out.writeLong(timeout);
        out.writeLong(ttl);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        dataKey = new Data();
        dataKey.readData(in);
        dataValue = IOUtil.readNullableData(in);
        threadId = in.readInt();
        timeout = in.readLong();
        ttl = in.readLong();
    }
}
