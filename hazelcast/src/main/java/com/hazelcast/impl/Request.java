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

package com.hazelcast.impl;

import com.hazelcast.impl.base.CallState;
import com.hazelcast.impl.base.CallStateAware;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

public class Request implements CallStateAware {

    enum ResponseType {
        OBJECT, BOOLEAN, LONG;
    }

    public final static long DEFAULT_TIMEOUT = -1;

    public final static long DEFAULT_TTL = -1;
    public final static int DEFAULT_REDO_COUNT = 0;
    public final static long DEFAULT_TXN_ID = -1;
    public final static int DEFAULT_LOCK_COUNT = 0;
    public final static int DEFAULT_LOCK_THREAD_ID = -1;
    public final static int DEFAULT_BLOCK_ID = -1;
    public final static long DEFAULT_CALL_ID = -1;
    public final static long DEFAULT_VERSION = -1;
    public String name = null;

    public Data key = null;
    public Data value = null;
    public long timeout = DEFAULT_TIMEOUT;
    public long ttl = DEFAULT_TTL;
    public boolean local = true;
    public boolean scheduled = false;
    public ClusterOperation operation;
    public Address caller = null;
    public int redoCount = DEFAULT_REDO_COUNT;
    public byte redoCode = 0;
    public Address lockAddress = null;
    public int lockCount = DEFAULT_LOCK_COUNT;
    public int lockThreadId = DEFAULT_LOCK_THREAD_ID;
    public int blockId = DEFAULT_BLOCK_ID;
    public long callId = DEFAULT_CALL_ID;
    public long longValue = Long.MIN_VALUE;
    public long version = DEFAULT_VERSION;
    public long txnId = DEFAULT_TXN_ID;
    public Long[] indexes;
    public byte[] indexTypes;
    public Object attachment = null;
    public Object response = null;
    public ResponseType responseType = ResponseType.OBJECT;
    public Record record = null;
    public CallState callState = null;
    public Address target = null;
    public long lastTime;

    public boolean hasEnoughTimeToSchedule() {
        return (timeout == -1) || (timeout > 100);
    }

    public CallState getCallState() {
        return callState;
    }

    @Override
    public String toString() {
        return "Request{" +
                "name='" + name + '\'' +
                "," + operation +
                ", redoCount='" + redoCount + '\'' +
                ", callId='" + callId + '\'' +
                ", timeout='" + timeout + '\'' +
                ", target='" + target + '\'' +
                ", lockThreadId='" + lockThreadId + '\'' +
                ", lockAddress='" + lockAddress + '\'' +
                ", local='" + local + '\'' +
                '}';
    }

    public void reset() {
        this.local = true;
        this.operation = ClusterOperation.NONE;
        this.name = null;
        this.key = null;
        this.value = null;
        this.blockId = DEFAULT_BLOCK_ID;
        this.timeout = DEFAULT_TIMEOUT;
        this.ttl = DEFAULT_TTL;
        this.txnId = DEFAULT_TXN_ID;
        this.callId = DEFAULT_CALL_ID;
        this.lockThreadId = DEFAULT_LOCK_THREAD_ID;
        this.lockAddress = null;
        this.lockCount = DEFAULT_LOCK_COUNT;
        this.caller = null;
        this.longValue = Long.MIN_VALUE;
        this.version = DEFAULT_VERSION;
        this.response = null;
        this.scheduled = false;
        this.attachment = null;
        this.redoCount = DEFAULT_REDO_COUNT;
        this.indexes = null;
        this.indexTypes = null;
        this.responseType = ResponseType.OBJECT;
        this.record = null;
        this.callState = null;
        this.target = null;
    }

    public void beforeRedo() {
        this.record = null;
        this.scheduled = false;
        this.response = null;
    }

    public void setIndexes(Long[] newIndexes, byte[] indexTypes) {
        this.indexes = newIndexes;
        this.indexTypes = indexTypes;
        if (indexes.length != indexTypes.length) {
            throw new RuntimeException("Indexes length and indexTypes length has to be the same."
                    + indexes.length + " vs. " + indexTypes.length);
        }
    }

    private void set(final boolean local, final ClusterOperation operation, final String name,
                     final Data key, final Data value, final int blockId, final long timeout, long ttl,
                     final long txnId, final long callId, final int lockThreadId,
                     final Address lockAddress, final int lockCount, final Address caller,
                     final long longValue, final long version, final int redoCount) {
        this.local = local;
        this.operation = operation;
        this.name = name;
        this.key = key;
        this.value = value;
        this.blockId = blockId;
        this.timeout = (timeout < 0) ? -1 : timeout;
        this.ttl = ttl;
        this.txnId = txnId;
        this.callId = callId;
        this.lockThreadId = lockThreadId;
        this.lockAddress = lockAddress;
        this.lockCount = lockCount;
        this.caller = caller;
        this.longValue = longValue;
        this.version = version;
        this.record = null;
        this.redoCount = redoCount;
    }

    public void setLocal(final ClusterOperation operation, final String name, final Data key,
                         final Data value, final int blockId, final long timeout, final long ttl,
                         final Address thisAddress) {
        //set the defaults here//
        this.response = null;
        this.scheduled = false;
        this.attachment = null;
//        this.redoCount = DEFAULT_REDO_COUNT;
        this.indexes = null;
        this.indexTypes = null;
        this.responseType = ResponseType.OBJECT;
        // set the values //
        set(true,
                operation,
                name,
                key,
                value,
                blockId,
                timeout,
                ttl,
                ThreadContext.get().getTxnId(),
                DEFAULT_CALL_ID,
                ThreadContext.get().getThreadId(),
                thisAddress,
                DEFAULT_LOCK_COUNT,
                thisAddress,
                Long.MIN_VALUE,
                DEFAULT_VERSION,
                DEFAULT_REDO_COUNT);
    }

    public static Request copyFromRequest(Request request) {
        final Request copy = new Request();
        copy.setFromRequest(request);
        return copy;
    }

    public void setFromRequest(Request req) {
        set(req.local, req.operation, req.name, req.key, req.value, req.blockId, req.timeout, req.ttl,
                req.txnId, req.callId, req.lockThreadId, req.lockAddress, req.lockCount,
                req.caller, req.longValue, req.version, req.redoCount);
        attachment = req.attachment;
        response = req.response;
        scheduled = req.scheduled;
        indexes = req.indexes;
        indexTypes = req.indexTypes;
    }

    public static Request copyFromPacket(Packet packet) {
        final Request copy = new Request();
        copy.setFromPacket(packet);
        return copy;
    }

    public void setFromPacket(Packet packet) {
        set(false, packet.operation, packet.name, packet.getKeyData(), packet.getValueData(),
            packet.blockId, packet.timeout, packet.ttl, packet.txnId, packet.callId, packet.threadId,
            packet.lockAddress, packet.lockCount, packet.conn.getEndPoint(), packet.longValue,
            packet.version, packet.redoData);
        indexes = packet.indexes;
        indexTypes = packet.indexTypes;
        callState = packet.callState;
    }

    public void setFromRecord(Record record) {
        reset();
        name = record.getName();
        version = record.getVersion();
        blockId = record.getBlockId();
        DistributedLock lock = record.getLock();
        if (lock != null) {
            lockThreadId = lock.getLockThreadId();
            lockAddress = lock.getLockAddress();
            lockCount = lock.getLockCount();
        }
        ttl = record.getRemainingTTL();
        timeout = record.getRemainingIdle();
        key = record.getKeyData();
        value = record.getValueData();
        if (record.getIndexes() != null) {
            setIndexes(record.getIndexes(), record.getIndexTypes());
        }
    }

    public void clearForResponse() {
        if (!this.local) {
            this.name = null;
            this.key = null;
            this.value = null;
            this.blockId = DEFAULT_BLOCK_ID;
            this.timeout = DEFAULT_TIMEOUT;
            this.ttl = DEFAULT_TTL;
            this.txnId = DEFAULT_TXN_ID;
            this.lockThreadId = DEFAULT_LOCK_THREAD_ID;
            this.lockAddress = null;
            this.lockCount = DEFAULT_LOCK_COUNT;
            this.longValue = Long.MIN_VALUE;
            this.version = DEFAULT_VERSION;
            this.indexes = null;
            this.indexTypes = null;
            this.record = null;
        }
    }

    public void setLongRequest() {
        responseType = ResponseType.LONG;
    }

    public void setBooleanRequest() {
        responseType = ResponseType.BOOLEAN;
    }

    public void setObjectRequest() {
        responseType = ResponseType.OBJECT;
    }

    public boolean isLongRequest() {
        return (responseType == ResponseType.LONG);
    }

    public boolean isBooleanRequest() {
        return (responseType == ResponseType.BOOLEAN);
    }

    public boolean isObjectRequest() {
        return (responseType == ResponseType.OBJECT);
    }
}
