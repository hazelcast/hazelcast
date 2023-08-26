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
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.opcodeToString;
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
 * handler id is needs to be made and then a {@link CompletionHandler}
 * is registered under this handler id. And when the cqe's are processed, a
 * lookup is done that translates the userdata into a handler and then the
 * {@link CompletionHandler#complete(int, int, long)}.
 * A CompletionQueue can only be accessed by the eventloop thread.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public final class CompletionQueue {

    public static final byte TYPE_GENERIC = 0;
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

    private final CompletionHandler[] generic_handlers;

    // an array that shows which positions in the handlers array are
    // not used.
    private final int[] generic_freeHandlers;
    private int generic_freeHandlersIndex;

    private UringEventloop.EventFdHandler eventFdHandler;
    private UringEventloop.TimeoutHandler timeoutHandler;
    private CompletionHandler storageHandler;

    private final UringAsyncServerSocket.Handler[] serverSockets_handlers;
    private final int[] serverSockets_freeHandlers;
    private int serverSockets_freeHandlersIndex;

    private final UringAsyncSocket.Handler[] sockets_handlers;
    private final int[] sockets_freeHandlers;
    private int sockets_freeHandlersIndex;

    CompletionQueue(Uring uring, int handlerCount) {
        this.uring = uring;
        this.generic_handlers = new CompletionHandler[handlerCount];
        this.generic_freeHandlers = new int[handlerCount];
        for (int k = 0; k < handlerCount; k++) {
            generic_freeHandlers[k] = k;
        }

        this.serverSockets_handlers = new UringAsyncServerSocket.Handler[1024];
        this.serverSockets_freeHandlers = new int[serverSockets_handlers.length];
        for (int k = 0; k < serverSockets_handlers.length; k++) {
            serverSockets_freeHandlers[k] = k;
        }

        this.sockets_handlers = new UringAsyncSocket.Handler[1024];
        this.sockets_freeHandlers = new int[sockets_handlers.length];
        for (int k = 0; k < sockets_handlers.length; k++) {
            sockets_freeHandlers[k] = k;
        }
    }

    public void register(CompletionHandler storageHandler) {
        this.storageHandler = storageHandler;
    }

    public void register(UringEventloop.EventFdHandler eventFdHandler) {
        this.eventFdHandler = checkNotNull(eventFdHandler, "eventFdHandler");
    }

    public void register(UringEventloop.TimeoutHandler timeoutHandler) {
        this.timeoutHandler = checkNotNull(timeoutHandler, "timeoutHandler");
    }

    static UncheckedIOException newCQEFailedException(String msg, String syscall, int opcode, int errnum) {
        return newUncheckedIOException(msg + " "
                + "Opcode " + opcodeToString(opcode) + " failed with error, "
                + "system call " + syscall + " '" + strerror(errnum) + "' erorcode=" + errorcode(errnum) + ". "
                + "First check " + toManPagesUrl("io_uring_enter(2)") + " section 'CQE ERRORS', "
                + "and then check " + toManPagesUrl(syscall) + " for more detail.");
    }

    public void register(UringAsyncServerSocket.Handler handler) {
        int index = serverSockets_freeHandlers[serverSockets_freeHandlersIndex];
        serverSockets_freeHandlersIndex++;
        serverSockets_handlers[index] = handler;
        handler.handlerIndex = index;
    }

    public void unregister(UringAsyncServerSocket.Handler handler) {
        serverSockets_handlers[handler.handlerIndex] = null;
        serverSockets_freeHandlersIndex--;
        serverSockets_freeHandlers[serverSockets_freeHandlersIndex] = handler.handlerIndex;
    }

    public void register(UringAsyncSocket.Handler handler) {
        int index = sockets_freeHandlers[sockets_freeHandlersIndex];
        sockets_freeHandlersIndex++;
        sockets_handlers[index] = handler;
        handler.handlerIndex = index;
    }

    // todo: fix magic numbers
    public static long encodeUserdata(byte type, byte opcode, int index) {
        return ((long) type << (5 * 8))
                + (((long) opcode) << (4 * 8))
                + index;
    }

    public static byte decodeOpcode(long userdata) {
        return (byte) ((userdata >> (4 * 8)) & 0xff);
    }

    public static int decodeIndex(long userdata) {
        return (int) (userdata & 0xFFFFFFFF);
    }

    public static byte decodeType(long userdata) {
        return (byte) ((userdata >> (5 * 8)) & 0xff);
    }

    // todo: remove
    public static void main(String[] args) {
        long userdata = encodeUserdata(TYPE_SOCKET, IORING_OP_CLOSE, 502);

        byte type = decodeType(userdata);
        byte opcode = decodeOpcode(userdata);
        int index = decodeIndex(userdata);

        System.out.println("Type match:" + (type == TYPE_SOCKET));
        System.out.println("opcode match:" + (opcode == IORING_OP_CLOSE));
        System.out.println("index match:" + (index == 502));
    }


    public void unregister(UringAsyncSocket.Handler handler) {
        sockets_handlers[handler.handlerIndex] = null;
        sockets_freeHandlersIndex--;
        sockets_freeHandlers[sockets_freeHandlersIndex] = handler.handlerIndex;
    }

    /**
     * Gets the next handler id. The handler id is typically used as user_data so that the
     * appropriate handler can be found based on the user_data in the cqe.
     *
     * @return the next handler id.
     */
    public int nextHandlerId() {
        int handlerId = generic_freeHandlers[generic_freeHandlersIndex];
        generic_freeHandlersIndex++;
        return handlerId;
    }

    /**
     * Registers a CompletionHandler with the given handlerId.
     *
     * @param handlerId the id to register the CompletionHandler on.
     * @param handler   the CompletionHandler to register.
     */
    public void register(int handlerId, CompletionHandler handler) {
        generic_handlers[handlerId] = handler;
    }

    /**
     * Unregisters the CompletionHandler with the given handlerId.
     *
     * @param handlerId the id of the CompletionHandler to remove.
     */
    public void unregister(int handlerId) {
        generic_handlers[handlerId] = null;
        generic_freeHandlersIndex--;
        generic_freeHandlers[generic_freeHandlersIndex] = handlerId;
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
     * completionHandler is called.
     * <p/>
     * The primary purpose this method exists is for benchmarking purposes
     * to exclude the overhead of the handler lookup.
     *
     * @param completionHandler callback for every completion entry.
     * @return the number of processed cqe's.
     */
    public int process(CompletionHandler completionHandler) {
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
                completionHandler.complete(res, flags, userdata);
            } catch (Exception e) {
                logger.severe("Failed to process " + completionHandler + " res:" + res + " flags:"
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
     * {@link CompletionHandler} is lookup up based on the userdata included in
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

            // todo: fix magic numbers
            byte type = decodeType(userdata);
            byte opcode = decodeOpcode(userdata);
            int index = decodeIndex(userdata);

//            System.out.println("completing " + userdata + " res:" + res + " type:" + type
//                    + " opcode:" + Uring.opcodeToString(opcode) + " index:" + index);

            try {
                switch (type) {
                    case TYPE_STORAGE:
                        storageHandler.complete(res, flags, userdata);
                        break;
                    case TYPE_TIMEOUT:
                        timeoutHandler.complete(opcode, res);
                        break;
                    case TYPE_EVENT_FD:
                        eventFdHandler.complete(opcode, res);
                        break;
                    case TYPE_GENERIC:
                        generic_handlers[index].complete(res, flags, userdata);
                        break;
                    case TYPE_SERVER_SOCKET:
                        serverSockets_handlers[index].complete(opcode, res);
                        break;
                    case TYPE_SOCKET:
                        sockets_handlers[index].complete(opcode, res);
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
}
