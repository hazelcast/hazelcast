/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.*;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalOperation;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 3/13/13
 */
public abstract class TransactionalMultiMapOperation extends TransactionalOperation
        implements KeyBasedOperation, BackupAwareOperation, WaitSupport {

    protected CollectionProxyId proxyId;
    protected long timeoutMillis;
    protected Data dataKey;
    protected int threadId;

    private transient CollectionContainer container;
    protected transient Object response;

    protected TransactionalMultiMapOperation() {
    }

    protected TransactionalMultiMapOperation(CollectionProxyId proxyId, Data dataKey, int threadId, String txnId, long timeoutMillis) {
        super(txnId);
        this.proxyId = proxyId;
        this.dataKey = dataKey;
        this.threadId = threadId;
        this.timeoutMillis = timeoutMillis;
    }

    protected final CollectionContainer getOrCreateContainer(){
        if (container == null){
            CollectionService service = getService();
            try {
                container = service.getOrCreateCollectionContainer(getPartitionId(), proxyId);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return container;
    }

    protected void process() throws TransactionException {
    }

    protected void prepare() throws TransactionException {
    }

    protected void commit() {
    }

    protected void rollback() {
    }

    public Object getResponse() {
        return response;
    }

    public final int getKeyHash() {
        return dataKey == null ? 0 : dataKey.getPartitionHash();
    }

    public final long getWaitTimeoutMillis() {
        return timeoutMillis;
    }

    public int getSyncBackupCount() {
        return getOrCreateContainer().getConfig().getSyncBackupCount();
    }

    public int getAsyncBackupCount() {
        return getOrCreateContainer().getConfig().getAsyncBackupCount();
    }

    public boolean shouldBackup(){
        return getState() == Transaction.State.COMMITTED;
    }

    public boolean shouldWait() {
        return !getOrCreateContainer().canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    public final void publishEvent(EntryEventType eventType, Data key, Object value) {
        NodeEngine engine = getNodeEngine();
        EventService eventService = engine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(CollectionService.SERVICE_NAME, proxyId.getName());
        for (EventRegistration registration : registrations) {
            CollectionEventFilter filter = (CollectionEventFilter) registration.getFilter();
            if (filter.getKey() == null || filter.getKey().equals(key)) {
                Data dataValue = filter.isIncludeValue() ? engine.toData(value) : null;
                CollectionEvent event = new CollectionEvent(proxyId, key, dataValue, eventType, engine.getThisAddress());
                eventService.publishEvent(CollectionService.SERVICE_NAME, registration, event);
            }
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        proxyId.writeData(out);
        dataKey.writeData(out);
        out.writeInt(threadId);
        out.writeLong(timeoutMillis);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        dataKey = new Data();
        dataKey.readData(in);
        threadId = in.readInt();
        timeoutMillis = in.readLong();
    }
}
