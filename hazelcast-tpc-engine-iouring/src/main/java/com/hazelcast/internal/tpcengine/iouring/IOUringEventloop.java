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
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.file.BlockDeviceRegistry;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.util.LongObjectHashMap;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_KERNEL_TIMESPEC;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeAllQuietly;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:MemberName",
        "checkstyle:DeclarationOrder",
        "checkstyle:NestedIfDepth",
        "checkstyle:MethodName"})
public final class IOUringEventloop extends Eventloop {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    protected static final int NANOSECONDS_IN_SECOND = 1_000_000_000;

    private final IOUringReactor ioUringReactor;
    private final BlockDeviceRegistry blockDeviceRegistry;
    final Map<BlockDevice, IOUringBlockRequestScheduler> deviceSchedulers = new HashMap<>();
    private final IOUring uring;

    final LongObjectHashMap<CompletionHandler> handlers = new LongObjectHashMap<>(4096);

    // this is not a very efficient allocator. It would be better to allocate a large chunk of
    // memory and then carve out smaller blocks. But for now it will do.
    private final IOBufferAllocator blockIOBufferAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());
    final SubmissionQueue sq;
    private final CompletionQueue cq;
    private final EventloopHandler eventLoopHandler;
    private final long userdata_eventRead;
    private final long userdata_timeout;
    private final long timeoutSpecAddr = UNSAFE.allocateMemory(SIZEOF_KERNEL_TIMESPEC);

    final EventFd eventfd = new EventFd();
    private final long eventFdReadBuf = UNSAFE.allocateMemory(SIZEOF_LONG);

    private long permanentHandlerIdGenerator;
    private long tmpHandlerIdGenerator = -1;

    public IOUringEventloop(IOUringReactor reactor, IOUringReactorBuilder builder) {
        super(reactor, builder);
        this.ioUringReactor = reactor;
        this.blockDeviceRegistry = builder.getBlockDeviceRegistry();

        // The uring instance needs to be created on the eventloop thread.
        // This is required for some of the setup flags.
        this.uring = new IOUring(builder.entries, builder.setupFlags);
        if (builder.registerRing) {
            this.uring.registerRingFd();
        }
        this.sq = uring.submissionQueue();
        this.cq = uring.completionQueue();

        this.eventLoopHandler = new EventloopHandler();
        this.userdata_eventRead = nextPermanentHandlerId();
        this.userdata_timeout = nextPermanentHandlerId();
        handlers.put(userdata_eventRead, new EventFdCompletionHandler());
        handlers.put(userdata_timeout, new TimeoutCompletionHandler());
    }

    /**
     * Gets the next handler id for a permanent handler. A permanent handler stays registered after receiving
     * a completion event.
     *
     * @return the next handler id.
     */
    public long nextPermanentHandlerId() {
        return permanentHandlerIdGenerator++;
    }

    /**
     * Gets the next handler id for a temporary handler. A temporary handler is automatically removed after receiving
     * the completion event.
     *
     * @return the next handler id.
     */
    public long nextTmpHandlerId() {
        return tmpHandlerIdGenerator--;
    }

    @Override
    public IOBufferAllocator blockIOBufferAllocator() {
        return blockIOBufferAllocator;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        checkNotNull(path, "path");

        BlockDevice dev = blockDeviceRegistry.findBlockDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path + "]");
        }

        IOUringBlockRequestScheduler blockRequestScheduler = deviceSchedulers.get(dev);
        if (blockRequestScheduler == null) {
            blockRequestScheduler = new IOUringBlockRequestScheduler(dev, this);
            deviceSchedulers.put(dev, blockRequestScheduler);
        }

        return new IOUringAsyncFile(path, this, blockRequestScheduler);
    }

    @Override
    public void beforeRun() {
        super.beforeRun();
        sq_offerEventFdRead();
    }

    @Override
    protected void park(long timeoutNanos) throws IOException {
        if (spin || timeoutNanos == 0) {
            sq.submit();
        } else {
            wakeupNeeded.set(true);
            if (scheduleBlockedGlobal()) {
                sq.submit();
            } else {
                if (timeoutNanos != Long.MAX_VALUE) {
                    sq_offerTimeout(timeoutNanos);
                }

                sq.submitAndWait();
            }
            wakeupNeeded.set(false);
        }

        if (cq.hasCompletions()) {
            cq.process(eventLoopHandler);
        }
    }

    @Override
    protected boolean ioSchedulerTick() {
        boolean worked = false;

        // todo: this is where we want to iterate over the dev schedulers and submit
        // the pending BlockRequests to the sq.

        if (sq.submit() > 0) {
            worked = true;
        }

        if (cq.hasCompletions()) {
            cq.process(eventLoopHandler);
            worked = true;
        }

        return worked;
    }

    // todo: delete
//    @Override
//    protected void run() throws Exception {
//        final NanoClock nanoClock = this.nanoClock;
//        final EventloopHandler eventLoopHandler = this.eventLoopHandler;
//        final AtomicBoolean wakeupNeeded = this.wakeupNeeded;
//        final CompletionQueue cq = this.cq;
//        final boolean spin = this.spin;
//        final SubmissionQueue sq = this.sq;
//        final Scheduler scheduler = this.scheduler;
//
//        sq_offerEventFdRead();
//
//        boolean moreWork = false;
//        do {
//            if (cq.hasCompletions()) {
//                // todo: do we want to control number of events being processed.
//                cq.process(eventLoopHandler);
//            } else {
//                if (spin || moreWork) {
//                    sq.submit();
//                } else {
//                    wakeupNeeded.set(true);
//                    if (hasConcurrentTask()) {
//                        sq.submit();
//                    } else {
//                        if (earliestDeadlineNanos != -1) {
//                            long timeoutNanos = earliestDeadlineNanos - nanoClock.nanoTime();
//                            if (timeoutNanos > 0) {
//                                sq_offerTimeout(timeoutNanos);
//                                sq.submitAndWait();
//                                nanoClock.update();
//                            } else {
//                                sq.submit();
//                            }
//                        } else {
//                            sq.submitAndWait();
//                            nanoClock.update();
//                        }
//                    }
//                    wakeupNeeded.set(false);
//                }
//            }
//
//            // what are the queues that are available for processing
//            // 1: completion events
//            // 2: concurrent task queue
//            // 3: timed task queue
//            // 4: local task queue
//            // 5: scheduler task queue
//
//            moreWork = tasksTick();
//            moreWork |= scheduler.tick();
//            moreWork |= scheduledTaskTick();
//        } while (!stop);
//    }

    @Override
    protected void destroy() {
        closeQuietly(uring);
        closeQuietly(eventfd);
        closeAllQuietly(ioUringReactor.closeables);
        ioUringReactor.closeables.clear();

        if (timeoutSpecAddr != 0) {
            UNSAFE.freeMemory(timeoutSpecAddr);
        }

        if (eventFdReadBuf != 0) {
            UNSAFE.freeMemory(eventFdReadBuf);
        }
    }

    // todo: I'm questioning of this is not going to lead to problems. Can it happen that
    // multiple timeout requests are offered? So one timeout request is scheduled while another command is
    // already in the pipeline. Then the thread waits, and this earlier command completes while the later
    // timeout command is still scheduled. If another timeout is scheduled, then you have 2 timeouts in the
    // uring and both share the same timeoutSpecAddr.
    private void sq_offerTimeout(long timeoutNanos) {
        if (timeoutNanos <= 0) {
            UNSAFE.putLong(timeoutSpecAddr, 0);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, 0);
        } else {
            long seconds = timeoutNanos / NANOSECONDS_IN_SECOND;
            UNSAFE.putLong(timeoutSpecAddr, seconds);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, timeoutNanos - seconds * NANOSECONDS_IN_SECOND);
        }

        // todo: return value isn't checked
        sq.offer(IORING_OP_TIMEOUT,
                0,
                0,
                -1,
                timeoutSpecAddr,
                1,
                0,
                userdata_timeout);
    }

    private void sq_offerEventFdRead() {
        // todo: we are not checking return value.
        sq.offer(IORING_OP_READ,
                0,
                0,
                eventfd.fd(),
                eventFdReadBuf,
                SIZEOF_LONG,
                0,
                userdata_eventRead);
    }


    private class EventloopHandler implements CompletionHandler {
        final LongObjectHashMap<CompletionHandler> handlers = IOUringEventloop.this.handlers;

        @Override
        public void handle(int res, int flags, long userdata) {
            // Temporary handlers have a userdata smaller than 0 and need to be removed
            // on completion.
            // Permanent handlers have a userdata equal or larger than 0 and should not
            // be removed on completion.
            CompletionHandler h = userdata >= 0
                    ? handlers.get(userdata)
                    : handlers.remove(userdata);

            if (h == null) {
                logger.warning("no handler found for: " + userdata);
            } else {
                h.handle(res, flags, userdata);
            }
        }
    }

    private class EventFdCompletionHandler implements CompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            sq_offerEventFdRead();
        }
    }

    private class TimeoutCompletionHandler implements CompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
        }
    }
}
