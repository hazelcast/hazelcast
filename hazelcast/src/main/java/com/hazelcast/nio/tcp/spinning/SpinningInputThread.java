/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.System.arraycopy;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class SpinningInputThread extends Thread {

    private static final ConnectionHandlers SHUTDOWN = new ConnectionHandlers();
    private static final AtomicReferenceFieldUpdater<SpinningInputThread, ConnectionHandlers> CONNECTION_HANDLERS
            = newUpdater(SpinningInputThread.class, ConnectionHandlers.class, "connectionHandlers");

    private final ILogger logger;
    private volatile ConnectionHandlers connectionHandlers;

    public SpinningInputThread(HazelcastThreadGroup threadGroup, ILogger logger) {
        super(threadGroup.getInternalThreadGroup(), "in-thread");
        this.logger = logger;
        this.connectionHandlers = new ConnectionHandlers();
    }

    public void addConnection(TcpIpConnection connection) {
        SpinningReadHandler readHandler = (SpinningReadHandler) connection.getReadHandler();

        for (; ; ) {
            ConnectionHandlers current = connectionHandlers;
            if (current == SHUTDOWN) {
                return;
            }

            int length = current.readHandlers.length;
            SpinningReadHandler[] newReadHandlers = new SpinningReadHandler[length + 1];
            arraycopy(current.readHandlers, 0, newReadHandlers, 0, length);
            newReadHandlers[length] = readHandler;

            ConnectionHandlers update = new ConnectionHandlers(newReadHandlers);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void removeConnection(TcpIpConnection connection) {
        SpinningReadHandler readHandler = (SpinningReadHandler) connection.getReadHandler();

        for (; ; ) {
            ConnectionHandlers current = connectionHandlers;
            if (current == SHUTDOWN) {
                return;
            }

            int indexOf = current.indexOf(readHandler);
            if (indexOf == -1) {
                return;
            }

            int length = current.readHandlers.length;
            SpinningReadHandler[] newReadHandlers = new SpinningReadHandler[length - 1];

            int destIndex = 0;
            for (int sourceIndex = 0; sourceIndex < length; sourceIndex++) {
                if (sourceIndex != indexOf) {
                    newReadHandlers[destIndex] = current.readHandlers[sourceIndex];
                    destIndex++;
                }
            }

            ConnectionHandlers update = new ConnectionHandlers(newReadHandlers);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void shutdown() {
        connectionHandlers = SHUTDOWN;
        interrupt();
    }

    @Override
    public void run() {
        for (; ; ) {
            ConnectionHandlers handlers = connectionHandlers;

            if (handlers == SHUTDOWN) {
                return;
            }

            for (SpinningReadHandler handler : handlers.readHandlers) {
                try {
                    handler.read();
                } catch (Throwable t) {
                    handler.onFailure(t);
                }
            }
        }
    }

    static class ConnectionHandlers {
        final SpinningReadHandler[] readHandlers;

        public ConnectionHandlers() {
            this(new SpinningReadHandler[0]);
        }

        public ConnectionHandlers(SpinningReadHandler[] readHandlers) {
            this.readHandlers = readHandlers;
        }

        public int indexOf(SpinningReadHandler readHandler) {
            for (int k = 0; k < readHandlers.length; k++) {
                if (readHandlers[k] == readHandler) {
                    return k;
                }
            }

            return -1;
        }
    }
}
