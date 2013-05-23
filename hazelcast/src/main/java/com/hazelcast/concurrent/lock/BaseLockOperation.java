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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

abstract class BaseLockOperation extends AbstractOperation implements PartitionAwareOperation {

    public static final long DEFAULT_LOCK_TTL = Long.MAX_VALUE;

    protected ObjectNamespace namespace;

    protected Data key;

    protected int threadId;

    protected long ttl = DEFAULT_LOCK_TTL;

    protected long timeout = -1;

    protected transient boolean response;

    private transient boolean asyncBackup = false;

    public BaseLockOperation() {
    }

    protected BaseLockOperation(ObjectNamespace namespace, Data key, int threadId) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
    }

    protected BaseLockOperation(ObjectNamespace namespace, Data key, int threadId, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        this.timeout = timeout;
    }

    public BaseLockOperation(ObjectNamespace namespace, Data key, int threadId, long ttl, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        this.ttl = ttl;
        this.timeout = timeout;
    }

    public final Object getResponse() {
        return response;
    }

    protected final LockStoreImpl getLockStore() {
        final LockService service = getService();
        return service.getLockStore(getPartitionId(), namespace);
    }

    public final int getSyncBackupCount() {
        return !asyncBackup ? getLockStore().getBackupCount() : 0;
    }

    public final int getAsyncBackupCount() {
        final LockStoreImpl lockStore = getLockStore();
        return !asyncBackup ? lockStore.getAsyncBackupCount() :
                lockStore.getBackupCount() + lockStore.getAsyncBackupCount();
    }

    public final void setAsyncBackup(boolean asyncBackup) {
        this.asyncBackup = asyncBackup;
    }

    @Override
    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    public final Data getKey() {
        return key;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        key.writeData(out);
        out.writeInt(threadId);
        out.writeLong(ttl);
        out.writeLong(timeout);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        key = new Data();
        key.readData(in);
        threadId = in.readInt();
        ttl = in.readLong();
        timeout = in.readLong();
    }
}
