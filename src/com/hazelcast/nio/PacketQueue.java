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

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.impl.Constants;
import com.hazelcast.impl.ThreadContext;

public class PacketQueue {

    private Logger logger = Logger.getLogger(PacketQueue.class.getName());

    private static final int PACKET_COUNT = 2000;

    public BlockingQueue<Packet> qPackets = new ArrayBlockingQueue<Packet>(PACKET_COUNT);

    private static final PacketQueue instance = new PacketQueue();

    private static final AtomicLong newPacketCount = new AtomicLong();

    private PacketQueue() {
        try {
            for (int i = 0; i < PACKET_COUNT; i++) {
                qPackets.add(new Packet());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static PacketQueue get() {
        return instance;
    }

    public Packet obtainPacket() {
        Packet packet = ThreadContext.get().getPacketPool().obtain();
        packet.reset();
        packet.released = false;
        return packet;
    }

    public void returnPacket(Packet packet) {
        packet.reset();
        if (packet.released) {
            logger.log(Level.SEVERE, "Already released packet!");
        }
        packet.released = true;
        ThreadContext.get().getPacketPool().release(packet);
    }

    public Packet createNewPacket() {
        long count = newPacketCount.incrementAndGet();
        if (count % 1000 == 0) {
            logger.log(Level.FINEST, "newPacketCount " + count);
        }
        return new Packet();
    }

    public final class Packet implements Constants, Constants.ResponseTypes {

        public String name;

        public int operation = -1;

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

        public byte responseType = RESPONSE_NONE;

        public boolean scheduled = false;

        public Object attachment;

        public long longValue = Long.MIN_VALUE;

        public long recordId = -1;

        public long version = -1;

        public long callId = -1;

        public Connection conn;

        public int totalSize = 0;

        public volatile boolean released = false;

        public Packet() {
        }

        public void write(ByteBuffer socketBB) {
            int totalWritten = 0;
            socketBB.put(bbSizes.array(), 0, bbSizes.limit());
            socketBB.put(bbHeader.array(), 0, bbHeader.limit());
            totalWritten += bbSizes.limit();
            totalWritten += bbHeader.limit();
            if (key.size() > 0) {
                totalWritten += key.copyToBuffer(socketBB);
            }
            if (value.size() > 0) {
                totalWritten += value.copyToBuffer(socketBB);
            }
            if (totalSize != totalWritten) {
                throw new RuntimeException(totalSize + " is totalSize but written: " + totalWritten);
            }
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
            return (bb.get() == (byte) 1) ? true : false;
        }

        public void write() {
            bbSizes.clear();
            bbHeader.clear();

            bbHeader.putInt(operation);
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
            operation = bbHeader.getInt();
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
            operation = -1;
            threadId = -1;
            lockCount = 0;
            lockAddress = null;
            timeout = -1;
            txnId = -1;
            responseType = RESPONSE_NONE;
            local = true;
            currentCallCount = 0;
            scheduled = false;
            blockId = -1;
            longValue = Long.MIN_VALUE;
            recordId = -1;
            version = -1;
            callId = -1;
            bbSizes.clear();
            bbHeader.clear();
            key.setNoData();
            value.setNoData();
            attachment = null;
            conn = null;
            totalSize = 0;
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

        public final boolean read(ByteBuffer bb) {
            while (!sizeRead && bbSizes.hasRemaining()) {
                BufferUtil.copy(bb, bbSizes);
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
                    BufferUtil.copy(bb, bbHeader);
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
            returnPacket(this);
        }

        public Object getValueObject() {
            return ThreadContext.get().toObject(value);
        }

        public Object getKeyObject() {
            return ThreadContext.get().toObject(key);
        }

        public void set(String name, int operation, Object objKey, Object objValue)
                throws Exception {
            this.threadId = Thread.currentThread().hashCode();
            this.name = name;
            this.operation = operation;
            if (objValue != null) {
                value = ThreadContext.get().toData(objValue);
            }
            if (objValue != null) {
                key = ThreadContext.get().toData(objKey);
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
}
