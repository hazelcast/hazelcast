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

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.net.NetworkScheduler;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_KERNEL_TIMESPEC;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The io_uring implementation of the {@link Eventloop}.
 */
public final class UringEventloop extends Eventloop {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long NS_PER_SECOND = SECONDS.toNanos(1);

    final Uring uring;
    final EventFdHandler eventFdHandler;
    private final SubmissionQueue submissionQueue;
    private final CompletionQueue completionQueue;
    private final TimeoutHandler timeoutHandler;

    private UringEventloop(Builder builder) {
        super(builder);

        UringReactor.Builder reactorBuilder = (UringReactor.Builder) builder.reactorBuilder;
        this.uring = builder.uring;
        if (reactorBuilder.registerRing) {
            uring.registerRingFd();
        }

        this.submissionQueue = uring.submissionQueue();
        this.completionQueue = uring.completionQueue();

        this.eventFdHandler = new EventFdHandler();
        eventFdHandler.handlerId = completionQueue.nextHandlerId();
        completionQueue.register(eventFdHandler.handlerId, eventFdHandler);

        this.timeoutHandler = new TimeoutHandler();
        timeoutHandler.handlerId = completionQueue.nextHandlerId();
        completionQueue.register(timeoutHandler.handlerId, timeoutHandler);
    }

    public NetworkScheduler networkScheduler() {
        return networkScheduler;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        checkNotNull(path, "path");
        return new UringAsyncFile(path, this, storageScheduler);
    }

    @Override
    public void beforeRun() {
        super.beforeRun();
        eventFdHandler.prepareSqe();
    }

    @Override
    protected void park(long timeoutNanos) {
        networkScheduler.tick();
        storageScheduler.tick();

        boolean skipPark = spin || timeoutNanos == 0;
        if (completionQueue.hasCompletions()) {
            skipPark |= true;
            completionQueue.process();
        }

        if (skipPark) {
            submissionQueue.submit();
        } else {
            wakeupNeeded.set(true);
            if (scheduler.hasOutsidePending() || networkScheduler.hasPending()) {
                submissionQueue.submit();
            } else {
                if (timeoutNanos != Long.MAX_VALUE) {
                    timeoutHandler.prepareSqe(timeoutNanos);
                }

                submissionQueue.submitAndWait();
            }
            wakeupNeeded.set(false);
        }

        if (completionQueue.hasCompletions()) {
            completionQueue.process();
        }
    }

    @Override
    protected boolean ioSchedulerTick() {
        networkScheduler.tick();
        storageScheduler.tick();

        boolean worked = false;

        if (submissionQueue.submit() > 0) {
            worked = true;
        }

        if (completionQueue.hasCompletions()) {
            completionQueue.process();
            worked = true;
        }

        return worked;
    }

    @Override
    protected void destroy() throws Exception {
        super.destroy();

        closeQuietly(uring);
        closeQuietly(eventFdHandler);
        closeQuietly(timeoutHandler);
    }

    final class EventFdHandler implements CompletionHandler, AutoCloseable {
        final EventFd eventFd = new EventFd();
        private final long readBufAddr = UNSAFE.allocateMemory(SIZEOF_LONG);
        private int handlerId;

        private void prepareSqe() {
            submissionQueue.add(IORING_OP_READ,
                    0,
                    0,
                    eventFd.fd(),
                    readBufAddr,
                    SIZEOF_LONG,
                    0,
                    handlerId);
        }

        @Override
        public void close() {
            closeQuietly(eventFd);
            UNSAFE.freeMemory(readBufAddr);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            prepareSqe();
        }
    }

    private class TimeoutHandler implements CompletionHandler, AutoCloseable {
        private int handlerId;
        private final long addr = UNSAFE.allocateMemory(SIZEOF_KERNEL_TIMESPEC);

        @Override
        public void close() {
            UNSAFE.freeMemory(addr);
        }

        // todo: I'm questioning of this is not going to lead to problems. Can
        // it happen that multiple timeout requests are offered? So one timeout
        // request is scheduled while another command is already in the pipeline.
        // Then the thread waits, and this earlier command completes while the later
        // timeout command is still scheduled. If another timeout is scheduled,
        // then you have 2 timeouts in the uring and both share the same
        // timeoutSpecAddr.
        // Perhaps it isn't a problem if the timeout data is copied by io_uring.
        private void prepareSqe(long timeoutNanos) {
            if (timeoutNanos <= 0) {
                UNSAFE.putLong(addr, 0);
                UNSAFE.putLong(addr + SIZEOF_LONG, 0);
            } else {
                long seconds = timeoutNanos / NS_PER_SECOND;
                UNSAFE.putLong(addr, seconds);
                UNSAFE.putLong(addr + SIZEOF_LONG, timeoutNanos - seconds * NS_PER_SECOND);
            }

            submissionQueue.add(IORING_OP_TIMEOUT,
                    0,
                    0,
                    -1,
                    addr,
                    1,
                    0,
                    handlerId);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
        }
    }

    @SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:TrailingComment"})
    public static class Builder extends Eventloop.Builder {

        public Uring uring;

        @Override
        protected void conclude() {
            super.conclude();

            if (networkScheduler == null) {
                networkScheduler = new UringFifoNetworkScheduler(reactorBuilder.socketsLimit);
            }

            if (uring == null) {
                // The uring instance needs to be created on the eventloop thread.
                // This is required for some of the setup flags.
                UringReactor.Builder reactorBuilder = (UringReactor.Builder) this.reactorBuilder;

                // The uring can be sized correctly based on the information we have.
                int entries
                        // 1 for reading and 1 for writing
                        = reactorBuilder.socketsLimit * 2
                        // every server socket needs 1 entry
                        + reactorBuilder.serverSocketsLimit
                        // all storage requests are preregistered; so even though we don't submit
                        // the requests, the completion queue needs to have at least that number
                        // of slots in the handler array.
                        // this logic needs to be fixed.
                        + reactorBuilder.storagePendingLimit
                        // eventFd
                        + 1
                        // timeout
                        + 1;
                this.uring = new Uring(nextPowerOfTwo(entries), reactorBuilder.setupFlags);
            }

            if (storageScheduler == null) {
                storageScheduler = new UringFifoStorageScheduler(
                        uring,
                        reactorBuilder.storageSubmitLimit,
                        reactorBuilder.storagePendingLimit);
            }
        }

        @Override
        protected UringEventloop construct() {
            return new UringEventloop(this);
        }
    }
}
