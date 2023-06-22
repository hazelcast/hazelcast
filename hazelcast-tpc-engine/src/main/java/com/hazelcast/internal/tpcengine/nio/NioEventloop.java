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
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.file.BlockRequestScheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.nio.NioBlockRequestScheduler.NioBlockRequest;
import com.hazelcast.internal.tpcengine.util.Preconditions;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Nio specific Eventloop implementation.
 */
final class NioEventloop extends Eventloop {

    private static final long NANOS_PER_MILLI = MILLISECONDS.toNanos(1);

    final Selector selector = SelectorOptimizer.newSelector();
    private final IOBufferAllocator blockIOBufferAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());

    private final Consumer<SelectionKey> selectorProcessor = key -> {
        NioHandler handler = (NioHandler) key.attachment();
        try {
            handler.handle();
        } catch (Exception e) {
            handler.close(null, e);
        }
    };

    NioEventloop(NioReactor reactor, NioReactorBuilder builder) {
        super(reactor, builder);
    }

    @Override
    public IOBufferAllocator blockIOBufferAllocator() {
        return blockIOBufferAllocator;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        Preconditions.checkNotNull(path, "path");

        BlockDevice dev = blockDeviceRegistry.findBlockDevice(path);
        if (dev == null) {
            throw newUncheckedIOException("Could not find storage device for [" + path + "]");
        }

        BlockRequestScheduler blockRequestScheduler = deviceSchedulers.get(dev);
        if (blockRequestScheduler == null) {
            blockRequestScheduler = new NioBlockRequestScheduler(dev, this);
            deviceSchedulers.put(dev, blockRequestScheduler);
        }

        return new NioAsyncFile(path, this, blockRequestScheduler);
    }

    @Override
    protected boolean ioSchedulerTick() throws IOException {
        int keyCount = selector.selectNow();
        boolean worked;

        if (keyCount == 0) {
            worked = false;
        } else {
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
            worked = true;
        }

        worked |= runDeviceSchedulerCompletions();

        return worked;
    }

    @Override
    protected void park(long timeoutNanos) throws IOException {
        boolean worked = runDeviceSchedulerCompletions();
        int keyCount;
        long timeoutMs = timeoutNanos / NANOS_PER_MILLI;
        if (spin || timeoutMs == 0 || worked) {
            keyCount = selector.selectNow();
        } else {
            wakeupNeeded.set(true);
            if (scheduleBlockedGlobal()) {
                keyCount = selector.selectNow();
            } else {
                keyCount = timeoutNanos == Long.MAX_VALUE
                        ? selector.select()
                        : selector.select(timeoutMs);
            }
            wakeupNeeded.set(false);
        }

        if (keyCount > 0) {
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

        runDeviceSchedulerCompletions();
    }

    private boolean runDeviceSchedulerCompletions() {
        // similar to cq completions in io_uring
        boolean worked = false;
        for (BlockRequestScheduler blockRequestScheduler : deviceSchedulers.values()) {
            NioBlockRequestScheduler scheduler = (NioBlockRequestScheduler) blockRequestScheduler;
            MpscArrayQueue<NioBlockRequest> cq = scheduler.cq;
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
//            if (scheduleBlockedGlobal()) {
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
}
