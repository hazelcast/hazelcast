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
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SIZEOF_KERNEL_TIMESPEC;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"checkstyle:MemberName",
        "checkstyle:DeclarationOrder",
        "checkstyle:NestedIfDepth",
        "checkstyle:MethodName"})
public final class IOUringEventloop extends Eventloop {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long NS_PER_SECOND = SECONDS.toNanos(1);

    final IOUring uring;
    private final SubmissionQueue sq;
    private final CompletionQueue cq;
    final EventFdHandler eventFdHandler;
    private final TimeoutHandler timeoutHandler;

    private IOUringEventloop(Builder builder) {
        super(builder);

        IOUringReactor.Builder reactorBuilder = (IOUringReactor.Builder) builder.reactorBuilder;
        this.uring = builder.uring;
        if (reactorBuilder.registerRing) {
            this.uring.registerRingFd();
        }

        this.sq = uring.sq();
        this.cq = uring.cq();

        eventFdHandler = new EventFdHandler();
        eventFdHandler.userdata = cq.nextPermanentHandlerId();
        cq.register(eventFdHandler.userdata, eventFdHandler);

        timeoutHandler = new TimeoutHandler();
        timeoutHandler.userdata = cq.nextPermanentHandlerId();
        cq.register(timeoutHandler.userdata, timeoutHandler);
    }


    @Override
    public AsyncFile newAsyncFile(String path) {
        checkNotNull(path, "path");

        StorageDevice dev = storageDeviceRegistry.findDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path + "]");
        }

        return new IOUringAsyncFile(path, this, storageScheduler, dev);
    }

    @Override
    public void beforeRun() {
        super.beforeRun();
        eventFdHandler.addRequest();
    }

    @Override
    protected void park(long timeoutNanos) {
        networkScheduler.tick();
        storageScheduler.tick();

        boolean completions = false;
        if (cq.hasCompletions()) {
            completions = true;
            cq.process();
        }

        if (spin || timeoutNanos == 0 || completions) {
            sq.submit();
        } else {
            wakeupNeeded.set(true);
            if (taskQueueScheduler.hasOutsidePending() || networkScheduler.hasPending()) {
                sq.submit();
            } else {
                if (timeoutNanos != Long.MAX_VALUE) {
                    timeoutHandler.addRequest(timeoutNanos);
                }

                sq.submitAndWait();
            }
            wakeupNeeded.set(false);
        }

        if (cq.hasCompletions()) {
            cq.process();
        }
    }

    @Override
    protected boolean ioSchedulerTick() {
        networkScheduler.tick();
        storageScheduler.tick();

        boolean worked = false;

        // todo: this is where we want to iterate over the dev schedulers and submit
        // the pending BlockRequests to the sq.

        if (sq.submit() > 0) {
            worked = true;
        }

        if (cq.hasCompletions()) {
            cq.process();
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
        private long userdata;
        final EventFd eventFd = new EventFd();
        private final long readBufAddr = UNSAFE.allocateMemory(SIZEOF_LONG);

        private void addRequest() {
            // todo: we are not checking return value.
            sq.offer(IORING_OP_READ,
                    0,
                    0,
                    eventFd.fd(),
                    readBufAddr,
                    SIZEOF_LONG,
                    0,
                    userdata);
        }

        @Override
        public void close() {
            closeQuietly(eventFd);
            UNSAFE.freeMemory(readBufAddr);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            addRequest();
        }
    }

    private class TimeoutHandler implements CompletionHandler, AutoCloseable {
        private long userdata;
        private final long addr = UNSAFE.allocateMemory(SIZEOF_KERNEL_TIMESPEC);

        @Override
        public void close() {
            UNSAFE.freeMemory(addr);
        }

        // todo: I'm questioning of this is not going to lead to problems. Can it happen that
        // multiple timeout requests are offered? So one timeout request is scheduled while another command is
        // already in the pipeline. Then the thread waits, and this earlier command completes while the later
        // timeout command is still scheduled. If another timeout is scheduled, then you have 2 timeouts in the
        // uring and both share the same timeoutSpecAddr.
        private void addRequest(long timeoutNanos) {
            if (timeoutNanos <= 0) {
                UNSAFE.putLong(addr, 0);
                UNSAFE.putLong(addr + SIZEOF_LONG, 0);
            } else {
                long seconds = timeoutNanos / NS_PER_SECOND;
                UNSAFE.putLong(addr, seconds);
                UNSAFE.putLong(addr + SIZEOF_LONG, timeoutNanos - seconds * NS_PER_SECOND);
            }

            // todo: return value isn't checked
            sq.offer(IORING_OP_TIMEOUT,
                    0,
                    0,
                    -1,
                    addr,
                    1,
                    0,
                    userdata);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
        }
    }

    // todo: remove magic number
    @SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:MagicNumber"})
    public static class Builder extends Eventloop.Builder {

        public IOUring uring;

        @Override
        protected void conclude() {
            super.conclude();

            if (networkScheduler == null) {
                networkScheduler = new IOUringNetworkScheduler(reactorBuilder.maxSockets);
            }

            if (uring == null) {
                // The uring instance needs to be created on the eventloop thread.
                // This is required for some of the setup flags.
                IOUringReactor.Builder reactorBuilder = (IOUringReactor.Builder) this.reactorBuilder;
                this.uring = new IOUring(reactorBuilder.entries, reactorBuilder.setupFlags);
            }

            if (storageScheduler == null) {
                storageScheduler = new IOUringFifoStorageScheduler(uring, 1024, 4096);
            }
        }

        @Override
        protected IOUringEventloop construct() {
            return new IOUringEventloop(this);
        }
    }
}
