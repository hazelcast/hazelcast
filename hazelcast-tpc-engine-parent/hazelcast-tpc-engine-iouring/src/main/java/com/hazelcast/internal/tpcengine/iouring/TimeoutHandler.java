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
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.TYPE_TIMEOUT;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.encodeUserdata;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_KERNEL_TIMESPEC;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The primary purpose of the timeout handler is to allow the reactor
 * to park with a timeout. So before it parks and there is a timeout,
 * it arms the timeout handler with that timeout which leads to a
 * sqe. And either the park will complete on that timeout sqe or
 * on some real work.
 * <p/>
 * This class is not thread-safe.
 */
public final class TimeoutHandler implements CompletionHandler {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long NS_PER_SECOND = SECONDS.toNanos(1);

    private final ByteBuffer timeoutBuffer;
    private final long timeoutAddr;
    private final TpcLogger logger;
    private final SubmissionQueue submissionQueue;

    public TimeoutHandler(Uring uring, TpcLogger logger) {
        this.logger = logger;
        this.submissionQueue = uring.submissionQueue();

        this.timeoutBuffer = allocateDirect(SIZEOF_KERNEL_TIMESPEC);
        this.timeoutAddr = addressOf(timeoutBuffer);

        uring.completionQueue().registerTimeoutHandler(this);
    }

    // todo: I'm questioning of this is not going to lead to problems. Can
    // it happen that multiple timeout requests are offered? So one timeout
    // request is scheduled while another command is already in the pipeline.
    // Then the thread waits, and this earlier command completes while the later
    // timeout command is still scheduled. If another timeout is scheduled,
    // then you have 2 timeouts in the uring and both share the same
    // timeoutSpec memory location.
    public void prepareTimeout(long timeoutNanos) {
        if (timeoutNanos <= 0) {
            UNSAFE.putLong(timeoutAddr, 0);
            UNSAFE.putLong(timeoutAddr + SIZEOF_LONG, 0);
        } else {
            long seconds = timeoutNanos / NS_PER_SECOND;
            UNSAFE.putLong(timeoutAddr, seconds);
            UNSAFE.putLong(timeoutAddr + SIZEOF_LONG, timeoutNanos - seconds * NS_PER_SECOND);
        }
        long userdata = encodeUserdata(TYPE_TIMEOUT, IORING_OP_TIMEOUT, 0);
        submissionQueue.prepareTimeout(timeoutAddr, userdata);
    }

    @Override
    public void complete(int res, int flags, long userdata) {
        // todo: negative res..
    }

}
