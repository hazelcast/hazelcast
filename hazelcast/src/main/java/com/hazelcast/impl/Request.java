/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.nio.Address;
import static com.hazelcast.nio.BufferUtil.doHardCopy;
import static com.hazelcast.nio.BufferUtil.doTake;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

class Request {
    String name = null;
    Data key = null;
    Data value = null;
    long timeout = -1;

    boolean local = true;
    boolean scheduled = false;
    ClusterOperation operation;

    Address caller = null;
    Address lockAddress = null;

    int redoCount = 0;
    int lockCount = 0;
    int lockThreadId = -1;
    int blockId = -1;
    long eventId = -1;
    long longValue = -1;
    long recordId = -1;
    long version = -1;
    long txnId = -1;
    byte indexCount = 0;
    long[] indexes = new long[6];
    Object attachment = null;
    Object response = null;


    public boolean hasEnoughTimeToSchedule() {
        return timeout == -1 || timeout > 100;
    }

    public void reset() {
        if (this.key != null) {
            this.key.setNoData();
        }
        if (this.value != null) {
            this.value.setNoData();
        }
        this.local = true;
        this.operation = ClusterOperation.NONE;
        this.name = null;
        this.key = null;
        this.value = null;
        this.blockId = -1;
        this.timeout = -1;
        this.txnId = -1;
        this.eventId = -1;
        this.lockThreadId = -1;
        this.lockAddress = null;
        this.lockCount = 0;
        this.caller = null;
        this.longValue = -1;
        this.response = null;
        this.scheduled = false;
        this.attachment = null;
        this.recordId = -1;
        this.version = -1;
        this.redoCount = 0;
        this.indexCount = 0;
    }

    public void setIndex(int index, long value) {
        indexes[index] = value;
        indexCount = (byte) (index+1);
    }

    public void set(final boolean local, final ClusterOperation operation, final String name,
                    final Data key, final Data value, final int blockId, final long timeout,
                    final long txnId, final long eventId, final int lockThreadId,
                    final Address lockAddress, final int lockCount, final Address caller,
                    final long longValue, final long recordId, final long version) {
        this.local = local;
        this.operation = operation;
        this.name = name;
        this.key = key;
        this.value = value;
        this.blockId = blockId;
        this.timeout = timeout;
        this.txnId = txnId;
        this.eventId = eventId;
        this.lockThreadId = lockThreadId;
        this.lockAddress = lockAddress;
        this.lockCount = lockCount;
        this.caller = caller;
        this.longValue = longValue;
        this.recordId = recordId;
        this.version = version;
    }

    public void setLocal(final ClusterOperation operation, final String name, final Data key,
                         final Data value, final int blockId, final long timeout, final long recordId,
                         final Address thisAddress) {
        reset();
        set(true, operation, name, key, value, blockId, timeout, -1, -1, -1, thisAddress, 0,
                thisAddress, -1, recordId, -1);
        this.txnId = ThreadContext.get().getTxnId();
        this.lockThreadId = Thread.currentThread().hashCode();
        this.caller = thisAddress;
    }

    public void setFromRequest(Request req, boolean hardCopy) {
        reset();
        set(req.local, req.operation, req.name, null, null, req.blockId, req.timeout,
                req.txnId, req.eventId, req.lockThreadId, req.lockAddress, req.lockCount,
                req.caller, req.longValue, req.recordId, req.version);
        if (hardCopy) {
            key = doHardCopy(req.key);
            value = doHardCopy(req.value);
        } else {
            key = req.key;
            value = req.value;
            req.key = null;
            req.value = null;
        }
        attachment = req.attachment;
        response = req.response;
        scheduled = req.scheduled;
    }

    public void setFromPacket(final Packet packet) {
        reset();
        set(false, packet.operation, packet.name, doTake(packet.key), doTake(packet.value),
                packet.blockId, packet.timeout, packet.txnId, packet.callId, packet.threadId,
                packet.lockAddress, packet.lockCount, packet.conn.getEndPoint(), packet.longValue,
                packet.recordId, packet.version);
        indexCount = packet.indexCount;
        for (int i=0; i < indexCount ; i++) {
            indexes[i] = packet.indexes[i];
        }

    }

    public Request hardCopy() {
        final Request copy = new Request();
        copy.setFromRequest(this, true);
        return copy;
    }
    
    public void setPacket(Packet packet) {
        packet.local = false;
        packet.operation = operation;
        packet.name = name;
        if (key != null)
            doHardCopy(key, packet.key);
        if (value != null)
            doHardCopy(value, packet.value);
        packet.blockId = blockId;
        packet.timeout = timeout;
        packet.txnId = txnId;
        packet.callId = eventId;
        packet.threadId = lockThreadId;
        packet.lockAddress = lockAddress;
        packet.lockCount = lockCount;
        packet.longValue = longValue;
        packet.recordId = recordId;
        packet.version = version;
        packet.indexCount = indexCount;
        for (int i=0; i < indexCount ; i++) {
            packet.indexes[i] = indexes[i];
        }
    }

}
