/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.tpc.engine.iobuffer;

import com.hazelcast.tpc.engine.AsyncSocket;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nio.Bits.BYTES_CHAR;
import static com.hazelcast.internal.nio.Bits.BYTES_INT;
import static com.hazelcast.internal.nio.Bits.BYTES_LONG;
import static com.hazelcast.internal.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;


// always
// size: int
// flags: int
// partitionId: int  : 8

// request
// callid: long: 12
// opcode: 20

// response
// call id: long 12
public class IOBuffer {

    public static final int FLAG_OP = 1 << 1;
    public static final int FLAG_OP_RESPONSE = 1 << 2;
    public static final int FLAG_OP_RESPONSE_CONTROL = 1 << 3;

    public static final int RESPONSE_TYPE_OVERLOAD = 0;
    public static final int RESPONSE_TYPE_EXCEPTION = 1;

    public CompletableFuture future;
    public IOBuffer next;
    public AsyncSocket socket;

    public static final int OFFSET_SIZE = 0;
    public static final int OFFSET_FLAGS = OFFSET_SIZE + BYTES_INT;
    public static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + BYTES_INT;

    public static final int OFFSET_REQ_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_REQ_OPCODE = OFFSET_REQ_CALL_ID + BYTES_LONG;
    public static final int OFFSET_REQ_PAYLOAD = OFFSET_REQ_OPCODE + BYTES_INT;

    public static final int OFFSET_RES_CALL_ID = OFFSET_PARTITION_ID + BYTES_INT;
    public static final int OFFSET_RES_PAYLOAD = OFFSET_RES_CALL_ID + BYTES_LONG;

    public boolean trackRelease;
    private ByteBuffer buff;
    public IOBufferAllocator allocator;
    public boolean concurrent = false;

    // make field?
    protected AtomicInteger refCount = new AtomicInteger();

    public IOBuffer() {
    }

    public IOBuffer(int size) {
        this(size, false);
    }

    public IOBuffer(int size, boolean direct) {
        //todo: allocate power of 2.
        this.buff = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    public IOBuffer(ByteBuffer buffer) {
        this.buff = buffer;
    }

    public IOBuffer newFuture() {
        this.future = new CompletableFuture();
        return this;
    }

    public void clear() {
        buff.clear();
    }

    public IOBuffer writeRequestHeader(int partitionId, int opcode) {
        ensureRemaining(OFFSET_REQ_PAYLOAD);
        buff.putInt(-1); //size
        buff.putInt(FLAG_OP);
        buff.putInt(partitionId);
        buff.putLong(-1); //callid
        buff.putInt(opcode);
        return this;
    }

    public IOBuffer writeResponseHeader(int partitionId, long callId) {
        return writeResponseHeader(partitionId, callId, 0);
    }

    public IOBuffer writeResponseHeader(int partitionId, long callId, int flags) {
        ensureRemaining(OFFSET_RES_PAYLOAD);
        buff.putInt(-1);  //size
        buff.putInt(FLAG_OP_RESPONSE | flags);
        buff.putInt(partitionId);
        buff.putLong(callId);
        return this;
    }

    public boolean isFlagRaised(int flag) {
        int flags = buff.getInt(OFFSET_FLAGS);
        return (flags & flag) != 0;
    }

    public IOBuffer addFlags(int addedFlags) {
        int oldFlags = buff.getInt(OFFSET_FLAGS);
        buff.putInt(OFFSET_FLAGS, oldFlags | addedFlags);
        return this;
    }

    public int flags() {
        return buff.getInt(OFFSET_FLAGS);
    }

    public ByteBuffer byteBuffer() {
        return buff;
    }

    public int size() {
        if (buff.limit() < BYTES_INT) {
            return -1;
        }
        return buff.getInt(0);
    }

    public void setSize(int size) {
        //ensure capacity?
        buff.putInt(0, size);
    }

    public IOBuffer writeByte(byte value) {
        ensureRemaining(BYTE_SIZE_IN_BYTES);
        buff.put(value);
        return this;
    }

    public IOBuffer writeChar(char value) {
        ensureRemaining(CHAR_SIZE_IN_BYTES);
        buff.putChar(value);
        return this;
    }

    public void setInt(int pos, int value) {
        buff.putInt(pos, value);
    }

    public IOBuffer writeInt(int value) {
        ensureRemaining(BYTES_INT);
        buff.putInt(value);
        return this;
    }

    public IOBuffer writeSizedBytes(byte[] src) {
        ensureRemaining(src.length + BYTES_INT);
        buff.putInt(src.length);
        buff.put(src);
        return this;
    }

    public IOBuffer writeBytes(byte[] src) {
        ensureRemaining(src.length);
        buff.put(src);
        return this;
    }

    public int position() {
        return buff.position();
    }

    // very inefficient
    public IOBuffer writeString(String s) {
        int length = s.length();

        ensureRemaining(BYTES_INT + length * BYTES_CHAR);

        buff.putInt(length);
        for (int k = 0; k < length; k++) {
            buff.putChar(s.charAt(k));
        }
        return this;
    }

    // very inefficient
    public void readString(StringBuffer sb) {
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
    }

    public String readString() {
        StringBuffer sb = new StringBuffer();
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
        return sb.toString();
    }

    public IOBuffer writeLong(long value) {
        ensureRemaining(BYTES_LONG);
        buff.putLong(value);
        return this;
    }

    public void putLong(int index, long value) {
        buff.putLong(index, value);
    }

    public long getLong(int index) {
        return buff.getLong(index);
    }

    public int readInt() {
        return buff.getInt();
    }

    public long readLong() {
        return buff.getLong();
    }

    public char readChar() {
        return buff.getChar();
    }

    public boolean isComplete() {
        if (buff.position() < BYTES_INT) {
            // not enough bytes.
            return false;
        } else {
            return buff.position() == buff.getInt(0);
        }
    }

    public IOBuffer reconstructComplete() {
        buff.flip();
        return this;
    }

    public IOBuffer constructComplete() {
        buff.putInt(OFFSET_SIZE, buff.position());
        buff.flip();
        return this;
    }

    public int getInt(int index) {
        return buff.getInt(index);
    }

    public byte getByte(int index) {
        return buff.get(index);
    }

    public IOBuffer write(ByteBuffer src, int count) {
        ensureRemaining(count);

        if (src.remaining() <= count) {
            buff.put(src);
        } else {
            int limit = src.limit();
            src.limit(src.position() + count);
            buff.put(src);
            src.limit(limit);
        }
        return this;
    }

    public IOBuffer position(int position) {
        buff.position(position);
        return this;
    }

    public IOBuffer incPosition(int delta) {
        buff.position(buff.position() + delta);
        return this;
    }

    public int remaining() {
        return buff.remaining();
    }

    public int refCount() {
        return refCount.get();
    }

    public void acquire() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current == 0) {
                    throw new IllegalStateException("Can't acquire a freed IOBuffer");
                }

                if (refCount.compareAndSet(current, current + 1)) {
                    break;
                }
            }
        } else {
            refCount.lazySet(refCount.get() + 1);
        }
    }

    public void release() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current > 0) {
                    if (refCount.compareAndSet(current, current - 1)) {
                        if (current == 1) {
                            allocator.free(this);
                        }
                        break;
                    }
                } else {
                    throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
                }
            }
        } else {
            int current = refCount.get();
            if (current == 1) {
                refCount.lazySet(0);
                allocator.free(this);
            } else if (current > 1) {
                refCount.lazySet(current - 1);
            } else {
                throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
            }
        }
    }

    public void ensureRemaining(int remaining) {
        if (buff.remaining() < remaining) {

            int newCapacity = nextPowerOfTwo(buff.capacity() + remaining);

            ByteBuffer newBuffer = buff.hasArray()
                    ? ByteBuffer.allocate(newCapacity)
                    : ByteBuffer.allocateDirect(newCapacity);
            buff.flip();
            newBuffer.put(buff);
            buff = newBuffer;
        }
    }

    public void readBytes(byte[] dst, int len) {
        buff.get(dst, 0, len);
    }
}
