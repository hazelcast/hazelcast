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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

abstract class BaseLockOperation extends AbstractOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    public static final long DEFAULT_LOCK_TTL = Long.MAX_VALUE;
    public static final int ANY_THREAD = 0;

    protected ObjectNamespace namespace;
    protected Data key;
    protected long threadId;
    protected long ttl = DEFAULT_LOCK_TTL;
    protected transient Object response;
    private transient boolean asyncBackup;

    public BaseLockOperation() {
    }

    protected BaseLockOperation(ObjectNamespace namespace, Data key, long threadId) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
    }

    protected BaseLockOperation(ObjectNamespace namespace, Data key, long threadId, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        setWaitTimeout(timeout);
    }

    public BaseLockOperation(ObjectNamespace namespace, Data key, long threadId, long ttl, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        this.ttl = ttl;
        setWaitTimeout(timeout);
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    protected final LockStoreImpl getLockStore() {
        LockServiceImpl service = getService();
        return service.getLockStore(getPartitionId(), namespace);
    }

    public final int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        } else {
            return getLockStore().getBackupCount();
        }
    }

    public final int getAsyncBackupCount() {
        LockStoreImpl lockStore = getLockStore();
        if (asyncBackup) {
            return lockStore.getBackupCount() + lockStore.getAsyncBackupCount();
        } else {
            return lockStore.getAsyncBackupCount();
        }
    }

    public final void setAsyncBackup(boolean asyncBackup) {
        this.asyncBackup = asyncBackup;
    }

    @Override
    public final String getServiceName() {
        return LockServiceImpl.SERVICE_NAME;
    }

    public final Data getKey() {
        return key;
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        out.writeData(key);
        out.writeLong(threadId);
        out.writeLong(ttl);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        key = in.readData();
        threadId = in.readLong();
        ttl = in.readLong();
    }
}
