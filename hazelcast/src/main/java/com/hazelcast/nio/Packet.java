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

import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.Constants;
import com.hazelcast.impl.ThreadContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class Packet {

    public String name;

    public ClusterOperation operation = ClusterOperation.NONE;

    public ByteBuffer bbSizes = ByteBuffer.allocate(12);

    public ByteBuffer bbHeader = ByteBuffer.allocate(500);

    public Data key = new Data();

    public Data value = new Data();

    public byte indexCount = 0;

    public long[] indexes = new long[10];

    public byte[] indexTypes = new byte[10];

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

    public long version = -1;

    public long callId = -1;

    public Connection conn;

    public int totalSize = 0;

    public boolean released = false;

    boolean sizeRead = false;

    int totalWritten = 0;

    public boolean client = false;

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
        if (key != null && key.size > 0) {
            key = new Data(ByteBuffer.wrap(key.buffer.array()));
        }
        if (value != null && value.size > 0) {
            value = new Data(ByteBuffer.wrap(value.buffer.array()));
        }
        bbHeader.putInt(operation.getValue());
        bbHeader.putInt(blockId);
        bbHeader.putInt(threadId);
        bbHeader.putInt(lockCount);
        bbHeader.putLong(timeout);
        bbHeader.putLong(txnId);
        bbHeader.putLong(longValue);
        bbHeader.putLong(version);
        bbHeader.putLong(callId);
        bbHeader.put((byte) (client ? 1 : 0));
        bbHeader.put(responseType);
        putString(bbHeader, name);
        boolean lockAddressNull = (lockAddress == null);
        writeBoolean(bbHeader, lockAddressNull);
        if (!lockAddressNull) {
            lockAddress.writeObject(bbHeader);
        }
        bbHeader.put(indexCount);
        for (int i = 0; i < indexCount; i++) {
            bbHeader.putLong(indexes[i]);
            bbHeader.put(indexTypes[i]);
        }
        bbHeader.flip();
        bbSizes.putInt(bbHeader.limit());
        bbSizes.putInt(key == null ? 0 : key.size);
        bbSizes.putInt(value == null ? 0 : value.size);
        bbSizes.flip();
        totalSize = 0;
        totalSize += bbSizes.limit();
        totalSize += bbHeader.limit();
        totalSize += key == null ? 0 : key.size;
        totalSize += value == null ? 0 : value.size;

    }

    public void read() {
        operation = ClusterOperation.create(bbHeader.getInt());
        blockId = bbHeader.getInt();
        threadId = bbHeader.getInt();
        lockCount = bbHeader.getInt();
        timeout = bbHeader.getLong();
        txnId = bbHeader.getLong();
        longValue = bbHeader.getLong();
        version = bbHeader.getLong();
        callId = bbHeader.getLong();
        client = (bbHeader.get() == 1);
        responseType = bbHeader.get();
        name = getString(bbHeader);
        boolean lockAddressNull = readBoolean(bbHeader);
        if (!lockAddressNull) {
            lockAddress = new Address();
            lockAddress.readObject(bbHeader);
        }
        indexCount = bbHeader.get();
        for (int i = 0; i < indexCount; i++) {
            indexes[i] = bbHeader.getLong();
            indexTypes[i] = bbHeader.get();
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
        version = -1;
        callId = -1;
        client = false;
        bbSizes.clear();
        bbHeader.clear();
        key = null;
        value = null;
        conn = null;
        totalSize = 0;
        totalWritten = 0;
        sizeRead = false;
        indexCount = 0;
    }

    @Override
    public String toString() {
        return "Packet " + operation + " name=" + name + "  local=" + local + "  blockId="
                + blockId + " data=" + value + " client=" + client;
    }

    public void flipBuffers() {
        bbSizes.flip();
        bbHeader.flip();
    }


    public final boolean writeToSocketBuffer(ByteBuffer dest) {
        totalWritten += BufferUtil.copyToDirectBuffer(bbSizes, dest);
        totalWritten += BufferUtil.copyToDirectBuffer(bbHeader, dest);
        if (key != null && key.size() > 0) {
            totalWritten += BufferUtil.copyToDirectBuffer(key.buffer, dest);
        }

        if (value != null && value.size() > 0) {
            totalWritten += BufferUtil.copyToDirectBuffer(value.buffer, dest);
        }
        return totalWritten >= totalSize;
    }


    public final boolean read(ByteBuffer bb) {
        while (!sizeRead && bb.hasRemaining() && bbSizes.hasRemaining()) {
            BufferUtil.copyToHeapBuffer(bb, bbSizes);
        }
        if (!sizeRead && !bbSizes.hasRemaining()) {
            sizeRead = true;
            bbSizes.flip();
            bbHeader.limit(bbSizes.getInt());
            key = new Data(bbSizes.getInt());
            value = new Data(bbSizes.getInt());
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

    public void set(String name, ClusterOperation operation, Object objKey, Object objValue) {
        this.threadId = ThreadContext.get().getThreadId();
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
        if (lockAddress == null) {
            lockAddress = conn.getEndPoint();
        }
    }

    public void setNoData() {
        key.setNoData();
        value.setNoData();
    }
}
