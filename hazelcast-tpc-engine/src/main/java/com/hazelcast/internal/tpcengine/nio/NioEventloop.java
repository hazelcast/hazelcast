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
import com.hazelcast.internal.tpcengine.Scheduler;
import com.hazelcast.internal.tpcengine.util.NanoClock;
import org.jctools.queues.MpmcArrayQueue;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Nio specific Eventloop implementation.
 */
class NioEventloop extends Eventloop {

    final Selector selector = SelectorOptimizer.newSelector();

    NioEventloop(NioReactor reactor, NioReactorBuilder builder) {
        super(reactor, builder);
    }

    @SuppressWarnings("java:S3776")
    @Override
    protected void run() throws Exception {
        final NanoClock nanoClock0 = nanoClock;
        final boolean spin0 = spin;
        final Selector selector0 = selector;
        final AtomicBoolean wakeupNeeded0 = wakeupNeeded;
        final MpmcArrayQueue externalTaskQueue0 = externalTaskQueue;
        final Scheduler scheduler0 = scheduler;

        boolean moreWork = false;
        do {
            int keyCount;
            if (spin0 || moreWork) {
                keyCount = selector0.selectNow();
            } else {
                wakeupNeeded0.set(true);
                if (externalTaskQueue0.isEmpty()) {
                    if (earliestDeadlineNanos == -1) {
                        keyCount = selector0.select();
                    } else {
                        long timeoutMillis = NANOSECONDS.toMillis(earliestDeadlineNanos - nanoClock0.nanoTime());
                        keyCount = timeoutMillis <= 0
                                ? selector0.selectNow()
                                : selector0.select(timeoutMillis);
                    }
                    // we need to update the clock because we could have been blocked for quite
                    // some time and clock could be very much out of sync.
                    nanoClock0.update();
                } else {
                    keyCount = selector0.selectNow();
                }
                wakeupNeeded0.set(false);
            }

            if (keyCount > 0) {
                Iterator<SelectionKey> it = selector0.selectedKeys().iterator();
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

            moreWork = runExternalTasks();
            moreWork |= scheduler0.tick();
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
