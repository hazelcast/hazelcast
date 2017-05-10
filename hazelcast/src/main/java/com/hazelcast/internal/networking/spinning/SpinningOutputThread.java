/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class SpinningOutputThread extends Thread {

    private static final SocketWriters SHUTDOWN = new SocketWriters();
    private static final AtomicReferenceFieldUpdater<SpinningOutputThread, SocketWriters> CONNECTION_HANDLERS
            = newUpdater(SpinningOutputThread.class, SocketWriters.class, "socketWriters");

    private volatile SocketWriters socketWriters;

    public SpinningOutputThread(String hzName) {
        super(ThreadUtil.createThreadName(hzName, "out-thread"));
        this.socketWriters = new SocketWriters();
    }

//    void addConnection(ChannelConnection connection) {
//        SpinningChannelWriter writer = (SpinningChannelWriter) connection.getChannelWriter();
//
//        for (; ; ) {
//            SocketWriters current = socketWriters;
//            if (current == SHUTDOWN) {
//                return;
//            }
//
//            int length = current.writers.length;
//
//            SpinningChannelWriter[] newWriters = new SpinningChannelWriter[length + 1];
//            arraycopy(current.writers, 0, newWriters, 0, length);
//            newWriters[length] = writer;
//
//            SocketWriters update = new SocketWriters(newWriters);
//            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
//                return;
//            }
//        }
//    }
//
//    void removeConnection(ChannelConnection connection) {
//        SpinningChannelWriter writeHandlers = (SpinningChannelWriter) connection.getChannelWriter();
//
//        for (; ; ) {
//            SocketWriters current = socketWriters;
//            if (current == SHUTDOWN) {
//                return;
//            }
//
//            int indexOf = current.indexOf(writeHandlers);
//            if (indexOf == -1) {
//                return;
//            }
//
//            int length = current.writers.length;
//            SpinningChannelWriter[] newWriters = new SpinningChannelWriter[length - 1];
//
//            int destIndex = 0;
//            for (int sourceIndex = 0; sourceIndex < length; sourceIndex++) {
//                if (sourceIndex != indexOf) {
//                    newWriters[destIndex] = current.writers[sourceIndex];
//                    destIndex++;
//                }
//            }
//
//            SocketWriters update = new SocketWriters(newWriters);
//            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
//                return;
//            }
//        }
//    }

    public void shutdown() {
        socketWriters = SHUTDOWN;
        interrupt();
    }

    @Override
    public void run() {
        for (; ; ) {
            SocketWriters handlers = socketWriters;

            if (handlers == SHUTDOWN) {
                return;
            }

            for (SpinningChannelWriter writer : handlers.writers) {
                try {
                    writer.write();
                } catch (Throwable t) {
                    writer.onFailure(t);
                }
            }
        }
    }

    private static class SocketWriters {
        final SpinningChannelWriter[] writers;

        SocketWriters() {
            this(new SpinningChannelWriter[0]);
        }

        SocketWriters(SpinningChannelWriter[] writers) {
            this.writers = writers;
        }

        public int indexOf(SpinningChannelWriter handler) {
            for (int k = 0; k < writers.length; k++) {
                if (writers[k] == handler) {
                    return k;
                }
            }

            return -1;
        }
    }
}
