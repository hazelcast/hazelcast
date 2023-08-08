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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.nio.NioStorageScheduler.NioStorageRequest;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Nio specific Eventloop implementation.
 */
final class NioEventloop extends Eventloop {

    private static final long NANOS_PER_MILLI = MILLISECONDS.toNanos(1);

    final Selector selector;

    private final Consumer<SelectionKey> selectorProcessor = key -> {
        NioHandler handler = (NioHandler) key.attachment();
        try {
            handler.handle();
        } catch (Exception e) {
            handler.close(null, e);
        }
    };

    private final NioNetworkScheduler nioNetworkScheduler;

    NioEventloop(NioEventloop.Builder builder) {
        super(builder);
        this.selector = builder.selector;
        this.nioNetworkScheduler = (NioNetworkScheduler) builder.networkScheduler;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        checkNotNull(path, "path");

        StorageDevice dev = storageDeviceRegistry.findDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path + "]");
        }

        StorageScheduler storageScheduler = storageSchedulers.get(dev);
        if (storageScheduler == null) {
            storageScheduler = new NioStorageScheduler(dev, this);
            storageSchedulers.put(dev, storageScheduler);
        }

        return new NioAsyncFile(path, this, storageScheduler);
    }

    @Override
    protected boolean ioSchedulerTick() throws IOException {
        nioNetworkScheduler.tick();

        int keyCount = selector.selectNow();
        boolean worked;

        if (keyCount == 0) {
            worked = false;
        } else {
            handleSelectedKeys();
            worked = true;
        }

        //worked |= runStorageCompletions();

        return worked;
    }

    private void handleSelectedKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            NioHandler handler = (NioHandler) key.attachment();
            try {
                handler.handle();
            } catch (Exception e) {
                handler.close(null, e);
            }
        }
    }

    @Override
    protected void park(long timeoutNanos) throws IOException {
        nioNetworkScheduler.tick();

        boolean worked = false;
        //runDeviceSchedulerCompletions();
        int keyCount;
        long timeoutMs = timeoutNanos / NANOS_PER_MILLI;
        if (spin || timeoutMs == 0 || worked) {
            keyCount = selector.selectNow();
        } else {
            wakeupNeeded.set(true);

            // It is critical that before we do a blocking select that we first
            // check for any 'outside' work that has been offered. Otherwise the
            // thread goes to sleep even though there is work that it should have
            // processed and this can lead to stalled behavior like stalled socket
            if (scheduleBlockedOutside() || nioNetworkScheduler.isDirty()) {
                keyCount = selector.selectNow();
            } else {
                keyCount = timeoutNanos == Long.MAX_VALUE
                        ? selector.select()
                        : selector.select(timeoutMs);
            }
            wakeupNeeded.set(false);
        }

        if (keyCount > 0) {
            handleSelectedKeys();
        }

        // todo: 2x
        //runDeviceSchedulerCompletions();
    }

    private boolean runStorageCompletions() {
        // similar to cq completions in io_uring
        boolean worked = false;

        // todo: litter
        for (StorageScheduler storageScheduler : storageSchedulers.values()) {
            NioStorageScheduler scheduler = (NioStorageScheduler) storageScheduler;
            MpscArrayQueue<NioStorageRequest> cq = scheduler.cq;
            int drained = cq.drain(scheduler::complete);
            if (drained > 0) {
                worked = true;
            }
        }
        return worked;
    }

//    @Override
//    protected boolean ioSchedulerTick() throws IOException {
//        return selector.selectNow(selectorProcessor) > 0;
//    }
//
//    @Override
//    protected void park(long timeoutNanos) throws IOException {
//        assert timeoutNanos >= 0;
//
//        long timeoutMs = timeoutNanos / NANOS_PER_MILLI;
//        if (spin || timeoutMs == 0) {
//            selector.selectNow(selectorProcessor);
//        } else {
//            wakeupNeeded.set(true);
//            if (scheduleBlockedOutside()) {
//                selector.selectNow(selectorProcessor);
//            } else if (timeoutNanos == Long.MAX_VALUE) {
//                selector.select(selectorProcessor, 0); //0 means block for ever.
//            } else {
//                selector.select(selectorProcessor, timeoutMs);
//            }
//            wakeupNeeded.set(false);
//        }
//    }

    @Override
    protected void destroy() {
        for (SelectionKey key : selector.keys()) {
            NioHandler handler = (NioHandler) key.attachment();

            if (handler == null) {
                // There is no handler; so lets cancel the key to be sure it gets cancelled.
                key.cancel();
            } else {
                // There is a handler; so it will take care of cancelling the key.
                try {
                    handler.close(reactor + " is terminating.", null);
                } catch (Exception e) {
                    logger.fine(e);
                }
            }
        }

        closeQuietly(selector);
    }

    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends Eventloop.Builder {
        public Selector selector;

        @Override
        protected void conclude() {
            super.conclude();

            if (networkScheduler == null) {
                networkScheduler = new NioNetworkScheduler(reactorBuilder.maxSockets);
            }

            if (selector == null) {
                selector = SelectorOptimizer.newSelector();
            }
        }

        @Override
        protected NioEventloop doBuild() {
            return new NioEventloop(this);
        }
    }
}
