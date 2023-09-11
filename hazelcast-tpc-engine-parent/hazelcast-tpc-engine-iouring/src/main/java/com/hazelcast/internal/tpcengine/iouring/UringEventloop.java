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

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link Eventloop}.
 */
public final class UringEventloop extends Eventloop {
    final Uring uring;
    final EventFdHandler eventFdHandler;
    final SubmissionQueue submissionQueue;
    final CompletionQueue completionQueue;
    final TimeoutHandler timeoutHandler;

    private UringEventloop(Builder builder) {
        super(builder);

        UringFifoNetworkScheduler networkScheduler = (UringFifoNetworkScheduler) builder.networkScheduler;
        networkScheduler.eventloopThread = builder.reactor.eventloopThread();

        UringReactor.Builder reactorBuilder = (UringReactor.Builder) builder.reactorBuilder;
        this.uring = builder.uring;
        this.submissionQueue = uring.submissionQueue();
        this.completionQueue = uring.completionQueue();
        this.eventFdHandler = new EventFdHandler(uring, logger);
        this.timeoutHandler = new TimeoutHandler(uring, logger);
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
        eventFdHandler.prepareRead();
    }

    @Override
    protected void park(long timeoutNanos) {
        long startMs = System.currentTimeMillis();

        metrics.incParkCount();

        networkScheduler.tick();
        storageScheduler.tick();

        boolean skipPark = timeoutNanos == 0;
        if (completionQueue.hasCompletions()) {
            skipPark = true;
            completionQueue.process();
        }

        if (skipPark) {
            submissionQueue.submit();
        } else {
            wakeupNeeded.set(true);
            if(networkScheduler.hasPending()){
                throw new RuntimeException();
            }
            if (signals.hasRaised()) {
                submissionQueue.submit();
            } else {
                if (timeoutNanos != Long.MAX_VALUE) {
                    timeoutHandler.prepareTimeout(timeoutNanos);
                }

                submissionQueue.submitAndWait();
            }
            wakeupNeeded.set(false);
        }

        if (completionQueue.hasCompletions()) {
            completionQueue.process();
        }

        long durationMs = System.currentTimeMillis()-startMs;
        System.out.println(reactor.name()+" park duration "+durationMs+ " ms");
    }

    @Override
    protected boolean ioTick() {
        metrics.incIoSchedulerTicks();

        boolean worked = false;

        worked |= networkScheduler.tick();
        worked |= storageScheduler.tick();

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

        // todo: make use of proper close
        closeQuietly(eventFdHandler);
        // todo only destroy this when everything else has completed.
        closeQuietly(uring);
    }

    @SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:TrailingComment"})
    public static class Builder extends Eventloop.Builder {

        public Uring uring;

        @Override
        protected void conclude() {
            super.conclude();

            UringReactor.Builder reactorBuilder = (UringReactor.Builder) this.reactorBuilder;

            if (uring == null) {
                // The uring instance needs to be created on the eventloop thread.
                // This is required for some of the setup flags.

                // The uring can be sized correctly based on the information we have.
                int entries
                        // 1 for reading and 1 for writing and 1 for close/shutdown
                        = reactorBuilder.socketsLimit * 3
                        // every server socket needs 1 entry for accept and 1 for close/shutdown
                        + reactorBuilder.serverSocketsLimit * 2
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

            if (reactorBuilder.registerRing) {
                uring.registerRingFd();
            }

            if (networkScheduler == null) {
                networkScheduler = new UringFifoNetworkScheduler(
                        uring,
                        reactorBuilder.socketsLimit,
                        reactorBuilder.serverSocketsLimit);
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
