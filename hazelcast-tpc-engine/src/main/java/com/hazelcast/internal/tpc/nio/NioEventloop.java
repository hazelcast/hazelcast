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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Scheduler;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.util.NanoClock;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Nio specific Eventloop implementation.
 */
class NioEventloop extends Eventloop {

    final Selector selector = SelectorOptimizer.newSelector();
    private final NioReactor reactor;

    NioEventloop(NioReactor reactor, NioReactorBuilder builder) {
        super(reactor, builder);
        this.reactor = reactor;
    }

    @Override
    public IOBufferAllocator fileIOBufferAllocator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void run() throws Exception {
        final NanoClock nanoClock = this.nanoClock;
        final boolean spin = this.spin;
        final Selector selector = this.selector;
        final AtomicBoolean wakeupNeeded = this.wakeupNeeded;
        final MpmcArrayQueue externalTaskQueue = this.externalTaskQueue;
        final Scheduler scheduler = this.scheduler;

        boolean moreWork = false;
        do {
            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (externalTaskQueue.isEmpty()) {
                    if (earliestDeadlineNanos == -1) {
                        keyCount = selector.select();
                    } else {
                        long timeoutMillis = NANOSECONDS.toMillis(earliestDeadlineNanos - nanoClock.nanoTime());
                        keyCount = timeoutMillis <= 0
                                ? selector.selectNow()
                                : selector.select(timeoutMillis);
                    }
                } else {
                    keyCount = selector.selectNow();
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
                    } catch (IOException e) {
                        handler.close(null, e);
                    }
                }
            }

            moreWork = runExternalTasks();
            moreWork |= scheduler.tick();
            moreWork |= runScheduledTasks();
            moreWork |= runLocalTasks();
        } while (!stop);
    }

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
