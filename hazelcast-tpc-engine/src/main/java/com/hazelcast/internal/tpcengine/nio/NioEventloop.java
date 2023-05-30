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
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;

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

    @Override
    public IOBufferAllocator blockIOBufferAllocator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean ioSchedulerTick() throws IOException {
        int keyCount = selector.selectNow();

        if (keyCount == 0) {
            return false;
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
            return true;
        }
    }

    @Override
    protected void park(long nowNanos) throws IOException {
        int keyCount;
        if (spin) {
            keyCount = selector.selectNow();
        } else {
            wakeupNeeded.set(true);

            if (scheduleExternalTaskGroups()) {
                keyCount = selector.selectNow();
            } else {
                long earliestDeadlineNanos = deadlineScheduler.earliestDeadlineNanos();
                if (earliestDeadlineNanos == -1) {
                    keyCount = selector.select();
                } else {
                    long timeoutMillis = NANOSECONDS.toMillis(earliestDeadlineNanos - nowNanos);
                    keyCount = timeoutMillis <= 0
                            ? selector.selectNow()
                            : selector.select(timeoutMillis);
                }
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
