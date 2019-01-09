/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.concurrent.lock.ObjectNamespaceSerializationHelper.readNamespaceCompatibly;
import static com.hazelcast.concurrent.lock.ObjectNamespaceSerializationHelper.writeNamespaceCompatibly;

public abstract class AbstractLockOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable, NamedOperation,
        ServiceNamespaceAware, Versioned {

    public static final int ANY_THREAD = 0;

    private static final AtomicLongFieldUpdater<AbstractLockOperation> REFERENCE_CALL_ID =
            AtomicLongFieldUpdater.newUpdater(AbstractLockOperation.class, "referenceCallId");

    protected ObjectNamespace namespace;
    protected Data key;
    protected long threadId;
    protected long leaseTime = -1L;
    protected transient Object response;
    private volatile long referenceCallId;
    private transient boolean asyncBackup;

    public AbstractLockOperation() {
    }

    protected AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
    }

    protected AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        setWaitTimeout(timeout);
    }

    public AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        this.leaseTime = leaseTime;
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
    protected void onSetCallId(long callId) {
        REFERENCE_CALL_ID.compareAndSet(this, 0, callId);
    }

    protected final long getReferenceCallId() {
        return referenceCallId != 0 ? referenceCallId : getCallId();
    }

    protected final void setReferenceCallId(long refCallId) {
        this.referenceCallId = refCallId;
    }

    @Override
    public String getServiceName() {
        return LockServiceImpl.SERVICE_NAME;
    }

    public final Data getKey() {
        return key;
    }

    @Override
    public String getName() {
        return namespace.getObjectName();
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        return namespace;
    }

    @Override
    public final int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        writeNamespaceCompatibly(namespace, out);
        out.writeData(key);
        out.writeLong(threadId);
        out.writeLong(leaseTime);
        out.writeLong(referenceCallId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = readNamespaceCompatibly(in);
        key = in.readData();
        threadId = in.readLong();
        leaseTime = in.readLong();
        referenceCallId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", namespace=").append(namespace);
        sb.append(", threadId=").append(threadId);
    }
}
