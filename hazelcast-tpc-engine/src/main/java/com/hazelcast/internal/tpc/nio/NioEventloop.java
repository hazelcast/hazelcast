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

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Unsafe;
import com.hazelcast.internal.tpc.util.NanoClock;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Nio implementation of the {@link Eventloop}.
 */
public final class NioEventloop extends Eventloop {

    final Selector selector = SelectorOptimizer.newSelector();

    public NioEventloop() {
        this(new NioEventloopBuilder());
    }

    public NioEventloop(NioEventloopBuilder eventloopBuilder) {
        super(eventloopBuilder);
    }

    @Override
    public AsyncServerSocket openTcpAsyncServerSocket() {
        return NioAsyncServerSocket.openTcpServerSocket(this);
    }

    @Override
    public AsyncSocket openTcpAsyncSocket() {
        return NioAsyncSocket.openTcpSocket();
    }

    @Override
    protected Unsafe createUnsafe() {
        return new NioUnsafe(this);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        boolean moreWork = false;
        NanoClock nanoClock = unsafe.nanoClock();
        do {
            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (concurrentTaskQueue.isEmpty()) {
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

                    SelectionKeyListener listener = (SelectionKeyListener) key.attachment();
                    try {
                        listener.handle(key);
                    } catch (IOException e) {
                        listener.close(null, e);
                    }
                }
            }

            moreWork = runConcurrentTasks();
            moreWork |= scheduler.tick();
            moreWork |= runScheduledTasks();
            moreWork |= runLocalTasks();
        } while (state == State.RUNNING);
    }

    @Override
    protected void afterEventloop() {
        for (SelectionKey key : selector.keys()) {
            SelectionKeyListener listener = (SelectionKeyListener) key.attachment();

            if (listener == null) {
                // There is no listener; so lets cancel the key to be sure it gets cancelled.
                key.cancel();
            } else {
                // There is a listener; so it will take care of cancelling the key.
                try {
                    listener.close(NioEventloop.this + " is terminating.", null);
                } catch (Exception e) {
                    logger.fine(e);
                }
            }
        }

        closeQuietly(selector);
    }

}
