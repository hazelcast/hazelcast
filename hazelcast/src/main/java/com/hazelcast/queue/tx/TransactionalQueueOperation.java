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

package com.hazelcast.queue.tx;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueueEvent;
import com.hazelcast.queue.QueueEventFilter;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalOperation;

import java.io.IOException;
import java.util.Collection;

/**
 * @mdogan 3/8/13
 */
public abstract class TransactionalQueueOperation extends TransactionalOperation
        implements KeyBasedOperation, BackupAwareOperation, WaitSupport, Notifier {

    protected String name;
    protected long timeoutMillis;

    private transient QueueContainer container;

    public TransactionalQueueOperation() {
    }

    protected TransactionalQueueOperation(String name, String txnId, long timeoutMillis) {
        super(txnId);
        this.name = name;
        this.timeoutMillis = timeoutMillis;
    }

    protected final QueueContainer getContainer() {
        if (container == null) {
            QueueService queueService = getService();
            try {
                container = queueService.getOrCreateContainer(name, this instanceof BackupOperation);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return container;
    }

    protected void process() throws TransactionException {

    }

    protected void onPrepare() throws TransactionException {

    }

    protected void onCommit() {

    }

    protected void onRollback() {

    }

    public final int getKeyHash() {
        return name.hashCode();
    }

    public final long getWaitTimeoutMillis() {
        return timeoutMillis;
    }

    public final int getSyncBackupCount() {
        return getContainer().getConfig().getSyncBackupCount();
    }

    public final int getAsyncBackupCount() {
        return getContainer().getConfig().getAsyncBackupCount();
    }

    public boolean shouldBackup() {
        return getState() == Transaction.State.COMMITTED;
    }

//    public boolean hasListener() {
//        EventService eventService = getNodeEngine().getEventService();
//        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
//        return registrations.size() > 0;
//    }

    public void publishEvent(ItemEventType eventType, Data data) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(getServiceName(), name);
        for (EventRegistration registration : registrations) {
            QueueEventFilter filter = (QueueEventFilter) registration.getFilter();
            QueueEvent event = new QueueEvent(name, filter.isIncludeValue() ? data : null, eventType, getNodeEngine().getThisAddress());
            eventService.publishEvent(getServiceName(), registration, event);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeLong(timeoutMillis);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        timeoutMillis = in.readLong();
    }
}
