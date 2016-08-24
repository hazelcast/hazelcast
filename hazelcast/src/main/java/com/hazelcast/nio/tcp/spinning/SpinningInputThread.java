/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.System.arraycopy;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class SpinningInputThread extends Thread {

    private static final SocketReaders SHUTDOWN = new SocketReaders();
    private static final AtomicReferenceFieldUpdater<SpinningInputThread, SocketReaders> CONNECTION_HANDLERS
            = newUpdater(SpinningInputThread.class, SocketReaders.class, "socketReaders");

    private volatile SocketReaders socketReaders;

    public SpinningInputThread(HazelcastThreadGroup threadGroup) {
        super(threadGroup.getInternalThreadGroup(), "in-thread");
        this.socketReaders = new SocketReaders();
    }

    public void addConnection(TcpIpConnection connection) {
        SpinningSocketReader reader = (SpinningSocketReader) connection.getSocketReader();

        for (; ; ) {
            SocketReaders current = socketReaders;
            if (current == SHUTDOWN) {
                return;
            }

            int length = current.readers.length;
            SpinningSocketReader[] newReaders = new SpinningSocketReader[length + 1];
            arraycopy(current.readers, 0, newReaders, 0, length);
            newReaders[length] = reader;

            SocketReaders update = new SocketReaders(newReaders);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void removeConnection(TcpIpConnection connection) {
        SpinningSocketReader reader = (SpinningSocketReader) connection.getSocketReader();

        for (; ; ) {
            SocketReaders current = socketReaders;
            if (current == SHUTDOWN) {
                return;
            }

            int indexOf = current.indexOf(reader);
            if (indexOf == -1) {
                return;
            }

            int length = current.readers.length;
            SpinningSocketReader[] newReaders = new SpinningSocketReader[length - 1];

            int destIndex = 0;
            for (int sourceIndex = 0; sourceIndex < length; sourceIndex++) {
                if (sourceIndex != indexOf) {
                    newReaders[destIndex] = current.readers[sourceIndex];
                    destIndex++;
                }
            }

            SocketReaders update = new SocketReaders(newReaders);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void shutdown() {
        socketReaders = SHUTDOWN;
        interrupt();
    }

    @Override
    public void run() {
        for (; ; ) {
            SocketReaders handlers = socketReaders;

            if (handlers == SHUTDOWN) {
                return;
            }

            for (SpinningSocketReader reader : handlers.readers) {
                try {
                    reader.read();
                } catch (Throwable t) {
                    reader.onFailure(t);
                }
            }
        }
    }

    static class SocketReaders {
        final SpinningSocketReader[] readers;

        public SocketReaders() {
            this(new SpinningSocketReader[0]);
        }

        public SocketReaders(SpinningSocketReader[] readers) {
            this.readers = readers;
        }

        public int indexOf(SpinningSocketReader readHandler) {
            for (int k = 0; k < readers.length; k++) {
                if (readers[k] == readHandler) {
                    return k;
                }
            }

            return -1;
        }
    }
}
