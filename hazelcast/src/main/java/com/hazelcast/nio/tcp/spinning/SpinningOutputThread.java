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

public class SpinningOutputThread extends Thread {

    private static final ConnectionHandlers SHUTDOWN = new ConnectionHandlers();
    private static final AtomicReferenceFieldUpdater<SpinningOutputThread, ConnectionHandlers> CONNECTION_HANDLERS
            = newUpdater(SpinningOutputThread.class, ConnectionHandlers.class, "connectionHandlers");

    private final ILogger logger;
    private volatile ConnectionHandlers connectionHandlers;

    public SpinningOutputThread(HazelcastThreadGroup threadGroup, ILogger logger) {
        super(threadGroup.getInternalThreadGroup(), "out-thread");
        this.logger = logger;
        this.connectionHandlers = new ConnectionHandlers();
    }

    public void addConnection(TcpIpConnection connection) {
        SpinningWriteHandler writeHandler = (SpinningWriteHandler) connection.getWriteHandler();

        for (; ; ) {
            ConnectionHandlers current = connectionHandlers;
            if (current == SHUTDOWN) {
                return;
            }

            int length = current.writeHandlers.length;

            SpinningWriteHandler[] newWriteHandlers = new SpinningWriteHandler[length + 1];
            arraycopy(current.writeHandlers, 0, newWriteHandlers, 0, length);
            newWriteHandlers[length] = writeHandler;

            ConnectionHandlers update = new ConnectionHandlers(newWriteHandlers);
            if (CONNECTION_HANDLERS.compareAndSet(this, current, update)) {
                return;
            }
        }
    }

    public void removeConnection(TcpIpConnection connection) {
        SpinningWriteHandler writeHandlers = (SpinningWriteHandler) connection.getWriteHandler();

        for (; ; ) {
            ConnectionHandlers current = connectionHandlers;
            if (current == SHUTDOWN) {
                return;
            }

            int indexOf = current.indexOf(writeHandlers);
            if (indexOf == -1) {
                return;
            }

            int length = current.writeHandlers.length;
            SpinningWriteHandler[] newWriteHandlers = new SpinningWriteHandler[length - 1];

            int destIndex = 0;
            for (int sourceIndex = 0; sourceIndex < length; sourceIndex++) {
                if (sourceIndex != indexOf) {
                    newWriteHandlers[destIndex] = current.writeHandlers[sourceIndex];
                    destIndex++;
                }
            }

            ConnectionHandlers update = new ConnectionHandlers(newWriteHandlers);
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

            for (SpinningWriteHandler handler : handlers.writeHandlers) {
                try {
                    handler.write();
                } catch (Throwable t) {
                    handler.onFailure(t);
                }
            }
        }
    }

    static class ConnectionHandlers {
        final SpinningWriteHandler[] writeHandlers;

        public ConnectionHandlers() {
            this(new SpinningWriteHandler[0]);
        }

        public ConnectionHandlers(SpinningWriteHandler[] writeHandlers) {
            this.writeHandlers = writeHandlers;
        }

        public int indexOf(SpinningWriteHandler handler) {
            for (int k = 0; k < writeHandlers.length; k++) {
                if (writeHandlers[k] == handler) {
                    return k;
                }
            }

            return -1;
        }
    }
}
