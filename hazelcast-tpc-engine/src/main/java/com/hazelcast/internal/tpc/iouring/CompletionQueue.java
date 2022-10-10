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

import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.UnsafeLocator;
import sun.misc.Unsafe;

public final class CompletionQueue {
    private final TpcLogger logger = TpcLoggerLocator.getLogger(CompletionQueue.class);

    public final static Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    public static final int OFFSET_CQE_USERDATA = 0;
    public static final int OFFSET_CQE_RES = 8;
    public static final int OFFSET_CQE_FLAGS = 12;

    public static final int CQE_SIZE = 16;

    private final IOUring uring;

    public int localHead;
    public int localTail;

    public long headAddr;
    public long tailAddr;
    public long cqesAddr;

    public int ringMask;

    CompletionQueue(IOUring uring) {
        this.uring = uring;
    }

    /**
     * Checks if there are any completion events.
     *
     * @return true if there are any completion events, false otherwise.
     */
    public boolean hasCompletions() {
        if (localHead != localTail) {
            return true;
        }

        localTail = UNSAFE.getIntVolatile(null, tailAddr);
        //System.out.println("hasCompletions count:"+(tail-head));
        return localHead != localTail;
    }

    /**
     * Processes all pending completions.
     *
     * @param completionHandler callback for every completed entry.
     * @return the number of processed completions.
     */
    public int process(IOCompletionHandler completionHandler) {
        // acquire load.
        localTail = UNSAFE.getIntVolatile(null, tailAddr);

        int processed = 0;
        while (localHead < localTail) {
            int index = localHead & ringMask;
            long cqeAddress = cqesAddr + index * CQE_SIZE;

            long userdata = UNSAFE.getLong(null, cqeAddress + OFFSET_CQE_USERDATA);
            int res = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_RES);
            int flags = UNSAFE.getInt(null, cqeAddress + OFFSET_CQE_FLAGS);

            try {
                completionHandler.handle(res, flags, userdata);
            } catch (Exception e) {
                logger.severe("Failed to process " + completionHandler + " res:" + res + " flags:"
                        + flags + " userdata:" + userdata, e);
            }

            localHead++;
            processed++;
        }

        //System.out.println("Cq::process processed:"+processed);

        // release-store.
        UNSAFE.putOrderedInt(null, headAddr, localHead);
        return processed;
    }

    public int ringMask() {
        return ringMask;
    }

    public IOUring getIoUring() {
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

    native void init(long io_uring);

    void onClose() {
        headAddr = 0;
        tailAddr = 0;
        cqesAddr = 0;
        ringMask = 0;
    }
}
