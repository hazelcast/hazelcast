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

import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.TYPE_EVENT_FD;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.encodeUserdata;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static java.nio.ByteBuffer.allocateDirect;

/**
 * The primary purpose of the {@link EventFdHandler} is to wakeup
 * the reactor when it is parked.
 * <p/>
 * This class is not thread-safe.
 */
public final class EventFdHandler implements AutoCloseable, CompletionHandler {

    private final TpcLogger logger;
    private final SubmissionQueue submissionQueue;
    private final EventFd eventFd = new EventFd();
    private final ByteBuffer readBufBuffer;
    private final long readBufAddr;

    public EventFdHandler(Uring uring, TpcLogger logger) {
        this.logger = logger;
        this.submissionQueue = uring.submissionQueue();

        this.readBufBuffer = allocateDirect(SIZEOF_LONG);
        this.readBufAddr = addressOf(readBufBuffer);

        uring.completionQueue().register(this);
    }

    public EventFd eventFd() {
        return eventFd;
    }

    public void prepareRead() {
        try {
            long userdata = encodeUserdata(TYPE_EVENT_FD, IORING_OP_READ, 0);
            submissionQueue.prepareRead(eventFd.fd(), readBufAddr, SIZEOF_LONG, userdata, userdata);
        } catch (Exception e) {
            logger.warning("Failed to prepare EventFd", e);
        }
    }

    @Override
    public void close() {
        // make use of a close.
        closeQuietly(eventFd);
    }

    @Override
    public void complete(int res, int flags, long userdata) {
        if (res >= 0) {
            prepareRead();
        } else {
            // todo: this can fail when closing.
            logger.warning("EventFd failed with error " + res);
        }
    }
}
