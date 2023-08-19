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
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;

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
 * {@link CompletionHandler#completeRequest(int, int, long)}.
 * <p/>
 * There are 2 flavors of handlers:
 * <ol>
 *     <li>temp handlers: handlers that automatically get unregistered
 *     after completion.</li>
 *     <li>permanent handlers: handlers that do not automatically get
 *     unregistered on completion.</li>
 * </ol>
 * <p/>
 * A CompletionQueue can only be accessed by the eventloop thread.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public final class CompletionQueue {

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

    private final CompletionHandler[] handlers;
    // an array that shows which positions in the handlers array are
    // not used.
    private final int[] freeHandlers;
    private int freeHandlersIndex;

    CompletionQueue(Uring uring, int handlerCount) {
        this.uring = uring;
        this.handlers = new CompletionHandler[handlerCount];
        this.freeHandlers = new int[handlerCount];
        for (int k = 0; k < handlerCount; k++) {
            freeHandlers[k] = k;
        }
    }

    static UncheckedIOException newCQEFailedException(String msg, String syscall, int opcode, int errnum) {
        return newUncheckedIOException(msg + " "
                + "Opcode " + opcodeToString(opcode) + " failed with error, "
                + "system call " + syscall + " '" + strerror(errnum) + "' erorcode=" + errorcode(errnum) + ". "
                + "First check " + toManPagesUrl("io_uring_enter(2)") + " section 'CQE ERRORS', "
                + "and then check " + toManPagesUrl(syscall) + " for more detail.");
    }

    /**
     * Gets the next handler id. The handler id is typically used as user_data so that the
     * appropriate handler can be found based on the user_data in the cqe.
     *
     * @return the next handler id.
     */
    public int nextHandlerId() {
        int handlerId = freeHandlers[freeHandlersIndex];
        freeHandlersIndex++;
        return handlerId;
    }

    /**
     * Registers a CompletionHandler with the given handlerId.
     *
     * @param handlerId the id to register the CompletionHandler on.
     * @param handler   the CompletionHandler to register.
     */
    public void register(int handlerId, CompletionHandler handler) {
        handlers[handlerId] = handler;
    }

    /**
     * Unregisters the CompletionHandler with the given handlerId.
     *
     * @param handlerId the id of the CompletionHandler to remove.
     */
    public void unregister(int handlerId) {
        handlers[handlerId] = null;
        freeHandlersIndex--;
        freeHandlers[freeHandlersIndex] = handlerId;
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
                completionHandler.completeRequest(res, flags, userdata);
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

            CompletionHandler handler = handlers[(int) userdata];
            if (handler == null) {
                logger.warning("no handler found for: " + userdata);
            } else {
                try {
                    handler.completeRequest(res, flags, userdata);
                } catch (Exception e) {
                    logger.severe("Failed to process " + handler + " res:" + res + " flags:"
                            + flags + " userdata:" + userdata, e);
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
