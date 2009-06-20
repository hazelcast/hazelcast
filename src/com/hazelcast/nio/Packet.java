/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.impl.Constants;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.ClusterOperation;

import java.nio.ByteBuffer;

public final class Packet {

    public String name;

    public ClusterOperation operation = ClusterOperation.NONE;

    public ByteBuffer bbSizes = ByteBuffer.allocate(12);

    public ByteBuffer bbHeader = ByteBuffer.allocate(500);

    public Data key = new Data();

    public Data value = new Data();

    public long txnId = -1;

    public int threadId = -1;

    public int lockCount = 0;

    public Address lockAddress = null;

    public long timeout = -1;

    public boolean local = true;

    public int currentCallCount = 0;

    public int blockId = -1;

    public byte responseType = Constants.ResponseTypes.RESPONSE_NONE;

    public long longValue = Long.MIN_VALUE;

    public long recordId = -1;

    public long version = -1;

    public long callId = -1;

    public Connection conn;

    public int totalSize = 0;

    public volatile boolean released = false;

    public Packet() {
    }

    protected void putString(ByteBuffer bb, String str) {
        byte[] bytes = str.getBytes();
        bb.putInt(bytes.length);
        bb.put(bytes);
    }

    protected String getString(ByteBuffer bb) {
        int size = bb.getInt();
        byte[] bytes = new byte[size];
        bb.get(bytes);
        return new String(bytes);
    }

    protected void writeBoolean(ByteBuffer bb, boolean value) {
        bb.put((value) ? (byte) 1 : (byte) 0);
    }

    protected boolean readBoolean(ByteBuffer bb) {
        return (bb.get() == (byte) 1);
    }

    public void write() {
        bbSizes.clear();
        bbHeader.clear();

        bbHeader.putInt(operation.getValue());
        bbHeader.putInt(blockId);
        bbHeader.putInt(threadId);
        bbHeader.putInt(lockCount);
        bbHeader.putLong(timeout);
        bbHeader.putLong(txnId);
        bbHeader.putLong(longValue);
        bbHeader.putLong(recordId);
        bbHeader.putLong(version);
        bbHeader.putLong(callId);
        bbHeader.put(responseType);
        putString(bbHeader, name);
        boolean lockAddressNull = (lockAddress == null);
        writeBoolean(bbHeader, lockAddressNull);
        if (!lockAddressNull) {
            lockAddress.writeObject(bbHeader);
        }

        bbHeader.flip();
        bbSizes.putInt(bbHeader.limit());
        bbSizes.putInt(key.size);
        bbSizes.putInt(value.size);
        bbSizes.flip();
        totalSize = 0;
        totalSize += bbSizes.limit();
        totalSize += bbHeader.limit();
        totalSize += key.size;
        totalSize += value.size;

    }

    public void read() {
        operation = ClusterOperation.create(bbHeader.getInt());
        blockId = bbHeader.getInt();
        threadId = bbHeader.getInt();
        lockCount = bbHeader.getInt();
        timeout = bbHeader.getLong();
        txnId = bbHeader.getLong();
        longValue = bbHeader.getLong();
        recordId = bbHeader.getLong();
        version = bbHeader.getLong();
        callId = bbHeader.getLong();
        responseType = bbHeader.get();
        name = getString(bbHeader);
        boolean lockAddressNull = readBoolean(bbHeader);
        if (!lockAddressNull) {
            lockAddress = new Address();
            lockAddress.readObject(bbHeader);
        }
    }

    public void reset() {
        name = null;
        operation = ClusterOperation.NONE;
        threadId = -1;
        lockCount = 0;
        lockAddress = null;
        timeout = -1;
        txnId = -1;
        responseType = Constants.ResponseTypes.RESPONSE_NONE;
        local = true;
        currentCallCount = 0;
        blockId = -1;
        longValue = Long.MIN_VALUE;
        recordId = -1;
        version = -1;
        callId = -1;
        bbSizes.clear();
        bbHeader.clear();
        key.setNoData();
        value.setNoData();
        conn = null;
        totalSize = 0;
        totalWritten = 0;
    }

    @Override
    public String toString() {
        return "Packet " + operation + " name=" + name + "  local=" + local + "  blockId="
                + blockId + " data=" + value;
    }

    public void flipBuffers() {
        bbSizes.flip();
        bbHeader.flip();
    }

    private boolean sizeRead = false;
    private int totalWritten = 0;

    public final boolean writeToSocketBuffer(ByteBuffer dest) {
        totalWritten += BufferUtil.copyToDirectBuffer(bbSizes, dest);
        totalWritten += BufferUtil.copyToDirectBuffer(bbHeader, dest);
        if (key.size() > 0) {
            int len = key.lsData.size();
            for (int i = 0; i < len & dest.hasRemaining(); i++) {
                ByteBuffer bb = key.lsData.get(i);
                totalWritten += BufferUtil.copyToDirectBuffer(bb, dest);
            }
        }

        if (value.size() > 0) {
            int len = value.lsData.size();
            for (int i = 0; i < len & dest.hasRemaining(); i++) {
                ByteBuffer bb = value.lsData.get(i);
                totalWritten += BufferUtil.copyToDirectBuffer(bb, dest);
            }
        }
        return totalWritten >= totalSize;
    }

    public final boolean read(ByteBuffer bb) {
        while (!sizeRead && bbSizes.hasRemaining()) {
            BufferUtil.copyToHeapBuffer(bb, bbSizes);
        }
        if (!sizeRead && !bbSizes.hasRemaining()) {
            sizeRead = true;
            bbSizes.flip();
            bbHeader.limit(bbSizes.getInt());
            key.size = bbSizes.getInt();
            value.size = bbSizes.getInt();
            if (bbHeader.limit() == 0) {
                throw new RuntimeException("read.bbHeader size cannot be 0");
            }
        }
        if (sizeRead) {
            while (bb.hasRemaining() && bbHeader.hasRemaining()) {
                BufferUtil.copyToHeapBuffer(bb, bbHeader);
            }

            while (bb.hasRemaining() && key.shouldRead()) {
                key.read(bb);
            }

            while (bb.hasRemaining() && value.shouldRead()) {
                value.read(bb);
            }
        }

        if (sizeRead && !bbHeader.hasRemaining() && !key.shouldRead() && !value.shouldRead()) {
            sizeRead = false;
            key.postRead();
            value.postRead();
            return true;
        }
        return false;
    }

    public void returnToContainer() {
        ThreadContext.get().getPacketPool().release(this);
    }

    public void set(String name, ClusterOperation operation, Object objKey, Object objValue)
            throws Exception {
        this.threadId = Thread.currentThread().hashCode();
        this.name = name;
        this.operation = operation;
        if (objKey != null) {
            key = ThreadContext.get().toData(objKey);
        }
        if (objValue != null) {
            value = ThreadContext.get().toData(objValue);
        }

    }

    public void setFromConnection(Connection conn) {
        this.conn = conn;
        if (lockAddress == null)
            lockAddress = conn.getEndPoint();
    }

    public void setNoData() {
        key.setNoData();
        value.setNoData();
    }
}
