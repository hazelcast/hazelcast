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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_ENTER_GETEVENTS;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_ENTER_REGISTERED_RING;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_NOP;
import static com.hazelcast.internal.tpc.iouring.Linux.errno;
import static com.hazelcast.internal.tpc.iouring.Linux.strerror;

// https://github.com/axboe/liburing/blob/master/src/include/liburing.h
public final class SubmissionQueue {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private static final int SIZE_SQE = 64;
    private static final int INT_SIZE = Integer.BYTES;

    public static final int OFFSET_SQE_opcode = 0;
    public static final int OFFSET_SQE_flags = 1;
    public static final int OFFSET_SQE_ioprio = 2;
    public static final int OFFSET_SQE_fd = 4;
    public static final int OFFSET_SQE_off = 8;
    public static final int OFFSET_SQE_addr = 16;
    public static final int OFFSET_SQE_len = 24;
    public static final int OFFSET_SQE_rw_flags = 28;
    public static final int OFFSET_SQE_user_data = 32;

    private final IOUring uring;
    public boolean ringBufferRegistered;
    int enterRingFd;
    int ringFd;

    long headAddr;
    long tailAddr;
    long sqesAddr;
    long arrayAddr;
    int ringMask;
    int ringEntries;
    int localTail;
    int localHead;

    SubmissionQueue(IOUring uring) {
        this.uring = uring;
        this.ringFd = uring.ringFd;
        this.enterRingFd = uring.enterRingFd;
    }

    public IOUring getIoUring() {
        return uring;
    }

    void init(long io_uring) {
        init0(io_uring);
        //UNSAFE.setMemory(sqesAddr, SIZE_SQE * (long)ringEntries, (byte) 0);
        localHead = acquireHead();
        localTail = acquireTail();
    }

    private native void init0(long io_uring);

    public int acquireHead() {
        return UNSAFE.getIntVolatile(null, headAddr);
    }

    public void releaseHead(int newHead) {
        UNSAFE.putOrderedInt(null, headAddr, newHead);
    }

    public int tail() {
        return UNSAFE.getInt(null, tailAddr);
    }

    public void tail(int tail) {
        UNSAFE.putInt(null, tailAddr, tail);
    }

    public int acquireTail() {
        return UNSAFE.getIntVolatile(null, tailAddr);
    }

    public void releaseTail(int tail) {
        UNSAFE.putOrderedInt(null, this.tailAddr, tail);
    }

    public void array(int pos, int value) {
        UNSAFE.putInt(arrayAddr + INT_SIZE * pos, value);
    }

    /**
     * Gets the free index in the submission queue.
     *
     * @return the next free index or -1 if there is no space.
     */
    public int nextIndex() {
        int size = localTail - localHead;
        if (size == ringEntries) {
            // there is no more space; but this is based on the cached head
            // lets update the localHead and try again.
            long oldLocalHead = localHead;
            // refresh the local head because it could be stale.
            localHead = acquireHead();
            if (oldLocalHead == localHead) {
                // there really is no space.
                return -1;
            }
        }

        int tail = localTail;
        //System.out.println("SubmissionQueue nextSqIndex:" + (tail & ringMask));
        localTail++;
        return tail & ringMask;
    }

    public void writeSqe(int index,
                         byte opcode,
                         int flags,
                         int rwFlags,
                         int fd,
                         long bufferAddress,
                         int length,
                         long offset,
                         long userData) {
        //System.out.println("writeSqe:" + index);
        long sqeAddr = sqesAddr + index * SIZE_SQE;
        UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, opcode);
        UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) flags);
        UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, fd);
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, offset);
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, bufferAddress);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, length);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, rwFlags);
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userData);
        //System.out.println("SubmissionQueue: userdata:" + userData + " index:" + index + " op:" + opcode);
    }

    public boolean offer(byte opcode,
                         int flags,
                         int rwFlags,
                         int fd,
                         long bufferAddress,
                         int length,
                         long offset,
                         long userdata) {
        int index = nextIndex();
        if (index == -1) {
            return false;
        }
        writeSqe(index, opcode, flags, rwFlags, fd, bufferAddress, length, offset, userdata);
        return true;
    }

    public boolean offerNop(long userdata) {
        int index = nextIndex();
        if (index == -1) {
            return false;
        } else {
            writeSqe(index, IORING_OP_NOP, 0, 0, 0, 0, 0, 0, userdata);
            return true;
        }
    }

    /**
     * @return the number of submitted entries.
     * <p>
     * https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html
     */
    public int enter(int toSubmit, int minComplete, int flags) {
        if (ringBufferRegistered) {
            flags |= IORING_ENTER_REGISTERED_RING;
        }

        return IOUring.enter(enterRingFd, toSubmit, minComplete, flags);
    }

    /**
     * Submits pending items and then waits for at least 1 completed event.
     *
     * @return the number of submitted items.
     */
    public int submitAndWait() {
        int tail = UNSAFE.getIntVolatile(null, tailAddr);
        int toSubmit = localTail - tail;
        UNSAFE.putOrderedInt(null, this.tailAddr, localTail);

        int flags = IORING_ENTER_GETEVENTS;
        if (ringBufferRegistered) {
            flags |= IORING_ENTER_REGISTERED_RING;
        }

        int res = IOUring.enter(enterRingFd, toSubmit, 1, flags);

        if (res >= 0) {
            return toSubmit;
        } else {
            throw new UncheckedIOException(new IOException(strerror(errno())));
        }
    }

    public int submit() {
        int tail = UNSAFE.getIntVolatile(null, tailAddr);
        int toSubmit = localTail - tail;

        //System.out.println("Sq::submit toSubmit "+toSubmit +" localTail:"+localTail+" tail:"+tail);

        if (toSubmit == 0) {
            return 0;
        } else {
            int flags = 0;
            if (ringBufferRegistered) {
                flags |= IORING_ENTER_REGISTERED_RING;
            }

            UNSAFE.putOrderedInt(null, this.tailAddr, localTail);

            int res = IOUring.enter(enterRingFd, toSubmit, 0, flags);
            if (res >= 0) {
                // System.out.println("Sq::submit res "+res);

                if (res != toSubmit) {
                    System.out.println("Not all items got submitted");
                }
                return toSubmit;
            } else {
                throw new UncheckedIOException(new IOException(strerror(errno())));
            }
        }
    }

    void onClose() {
        ringFd = -1;
        headAddr = 0;
        tailAddr = 0;
        sqesAddr = 0;
        arrayAddr = 0;
        ringMask = 0;
        ringEntries = 0;
    }
}
