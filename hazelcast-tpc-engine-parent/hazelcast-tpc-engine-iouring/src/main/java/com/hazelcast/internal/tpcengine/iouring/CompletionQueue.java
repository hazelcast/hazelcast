/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpcengine.iouring.Linux.errorcode;
import static com.hazelcast.internal.tpcengine.iouring.Linux.strerror;
import static com.hazelcast.internal.tpcengine.iouring.Linux.toManPagesUrl;
import static com.hazelcast.internal.tpcengine.iouring.Uring.opcodeToString;
import static com.hazelcast.internal.tpcengine.util.BitUtil.BITS_IN_BYTE;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * Represent the io_uring ringbuffer for completion queue events (cqe's).
 * <p/>
 * Each sqe (submission queue event) has a userdata field where a userdata
 * value can be passed. And on completion, the userdata is included. This
 * way the sqe and cqe can be correlated. In C you can pass anything like
 * a 'long int' or a pointer. Unfortunately in Java reference to objects
 * can't be passed as userdata. So what is done is that first a unique
 * handler id is needs to be made and then a {@link CompletionCallback}
 * is registered under this handler id. And when the cqe's are processed, a
 * lookup is done that translates the userdata into a handler and then the
 * {@link CompletionCallback#complete(int, int, long)}.
 * A CompletionQueue can only be accessed by the eventloop thread.
 */
// todo: fix magic number
@SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:MemberName", "checkstyle:MagicNumber"})
public final class CompletionQueue {

    public static final byte TYPE_SERVER_SOCKET = 1;
    public static final byte TYPE_SOCKET = 2;
    public static final byte TYPE_FILE = 3;
    public static final byte TYPE_EVENT_FD = 4;
    public static final byte TYPE_TIMEOUT = 5;
    public static final byte TYPE_STORAGE = 6;

    public static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    public static final int OFFSET_CQE_USERDATA = 0;
    public static final int OFFSET_CQE_RES = 8;
    public static final int OFFSET_CQE_FLAGS = 12;
    public static final int CQE_SIZE = 16;

    public int head;

    public long headAddr;
    public long tailAddr;
    public long cqesAddr;

    public int ringMask;

    private final TpcLogger logger = TpcLoggerLocator.getLogger(CompletionQueue.class);
    private final Uring uring;

    private CompletionCallback eventFdHandler;
    private CompletionCallback timeoutHandler;
    private CompletionCallback storageHandler;
    private CompletionCallback socketHandler;
    private CompletionCallback serverSocketHandler;

    CompletionQueue(Uring uring) {
        this.uring = uring;
    }

    public void registerStorageHandler(CompletionCallback storageHandler) {
        this.storageHandler = checkNotNull(storageHandler, "storageHandler");
    }

    public void registerSocketHandler(CompletionCallback socketHandler) {
        this.socketHandler = checkNotNull(socketHandler, "socketHandler");
    }

    public void registerServerSocketHandler(CompletionCallback serverSocketHandler) {
        this.serverSocketHandler = checkNotNull(serverSocketHandler, "serverSocketHandler");
    }

    public void registerEventFdHandler(CompletionCallback eventFdHandler) {
        this.eventFdHandler = checkNotNull(eventFdHandler, "eventFdHandler");
    }

    public void registerTimeoutHandler(CompletionCallback timeoutHandler) {
        this.timeoutHandler = checkNotNull(timeoutHandler, "timeoutHandler");
    }

    static UncheckedIOException newCQEFailedException(String msg, String syscall, int opcode, int errnum) {
        return newUncheckedIOException(msg + " "
                + "Opcode " + opcodeToString(opcode) + " failed with error, "
                + "system call " + syscall + " '" + strerror(errnum) + "' erorcode=" + errorcode(errnum) + ". "
                + "First check " + toManPagesUrl("io_uring_enter(2)") + " section 'CQE ERRORS', "
                + "and then check " + toManPagesUrl(syscall) + " for more detail.");
    }


    // todo: fix magic numbers
    public static long encodeUserdata(byte type, byte opcode, int index) {
        return ((long) type << (5 * BITS_IN_BYTE))
                + (((long) opcode) << (4 * BITS_IN_BYTE))
                + index;
    }

    public static byte decodeOpcode(long userdata) {
        return (byte) ((userdata >> (4 * BITS_IN_BYTE)) & 0xff);
    }

    public static int decodeIndex(long userdata) {
        return (int) (userdata & 0xFFFFFFFF);
    }

    public static byte decodeType(long userdata) {
        return (byte) ((userdata >> (5 * BITS_IN_BYTE)) & 0xff);
    }

    /**
     * Checks if there are any completion events.
     *
     * @return true if there are any completion events, false otherwise.
     */
    public boolean hasCompletions() {
        return head != UNSAFE.getIntVolatile(null, tailAddr);
    }

    /**
     * Processes all completion queue events (cqe). For every cqe, the
     * completionCallback is called.
     * <p/>
     * The primary purpose this method exists is for benchmarking purposes
     * to exclude the overhead of the handler lookup.
     *
     * @param completionCallback callback for every completion entry.
     * @return the number of processed cqe's.
     */
    public int process(CompletionCallback completionCallback) {
        // acquire load.
        int tail = UNSAFE.getIntVolatile(null, tailAddr);
        int readyCnt = tail - head;

        int processed = 0;
        for (int k = 0; k < readyCnt; k++) {
            int cqeIndex = head & ringMask;
            long cqeAddress = cqesAddr + (long) cqeIndex * CQE_SIZE;

            long userdata = UNSAFE.getLong(null, cqeAddress + OFFSET_CQE_USERDATA);

            int res = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_RES);
            int flags = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_FLAGS);

            try {
                completionCallback.complete(res, flags, userdata);
            } catch (Exception e) {
                logger.severe("Failed to process " + completionCallback + " res:" + res + " flags:"
                        + flags + " userdata:" + userdata, e);
            }

            head++;
            processed++;
        }

        //System.out.println("Cq::process processed:"+processed);

        // release-store.
        UNSAFE.putOrderedInt(null, headAddr, head);
        return processed;
    }

    /**
     * Processes all completion queue events (cqe). For every cqe, the
     * {@link CompletionCallback} is lookup up based on the userdata included in
     * the cqe and then called. If the handler doesn't exist, the completion is
     * ignored.
     *
     * @return the number of processed cqe's.
     */
    @SuppressWarnings({"checkstyle:AvoidNestedBlocks"})
    public int process() {
        // acquire load.
        int tail = UNSAFE.getIntVolatile(null, tailAddr);

        int readyCnt = tail - head;
        int processed = 0;

        for (int k = 0; k < readyCnt; k++) {
            int cqeIndex = head & ringMask;
            long cqeAddress = cqesAddr + (long) cqeIndex * CQE_SIZE;

            long userdata = UNSAFE.getLong(null, cqeAddress + OFFSET_CQE_USERDATA);
            int res = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_RES);
            int flags = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_FLAGS);
            byte type = decodeType(userdata);

//            System.out.println("completing " + userdata + " res:" + res + " type:" + type
//                    + " opcode:" + Uring.opcodeToString(opcode) + " index:" + index);

            try {
                switch (type) {
                    case TYPE_STORAGE:
                        storageHandler.complete(res, flags, userdata);
                        break;
                    case TYPE_TIMEOUT:
                        timeoutHandler.complete(res, flags, res);
                        break;
                    case TYPE_EVENT_FD:
                        eventFdHandler.complete(res, flags, res);
                        break;
                    case TYPE_SERVER_SOCKET:
                        serverSocketHandler.complete(res, flags, userdata);
                        break;
                    case TYPE_SOCKET:
                        socketHandler.complete(res, flags, userdata);
                        break;
                    default:
                        throw new IllegalArgumentException("Unrecognized type:" + type);
                }
            } catch (Exception e) {
                // The handlers should take care of exception handling; but if that failed
                // we have a catch all so that the eventloop is more robust.
                if (logger.isWarningEnabled()) {
                    logger.warning("Failed to process handler", e);
                }
            }

            head++;
            processed++;
        }

        // release-store.
        UNSAFE.putOrderedInt(null, headAddr, head);
        return processed;
    }

    public int ringMask() {
        return ringMask;
    }

    public Uring uring() {
        return uring;
    }

    public int acquireHead() {
        return UNSAFE.getIntVolatile(null, headAddr);
    }

    public void releaseHead(int newHead) {
        UNSAFE.putOrderedInt(null, headAddr, newHead);
    }

    public int acquireTail() {
        return UNSAFE.getIntVolatile(null, tailAddr);
    }

    native void init(long uring);

    void onClose() {
        headAddr = 0;
        tailAddr = 0;
        cqesAddr = 0;
        ringMask = 0;
    }

    /**
     * Callback interface to consume the completion events from the
     * {@link CompletionQueue}.
     */
    public interface CompletionCallback {

        void complete(int res, int flags, long userdata);
    }
}
