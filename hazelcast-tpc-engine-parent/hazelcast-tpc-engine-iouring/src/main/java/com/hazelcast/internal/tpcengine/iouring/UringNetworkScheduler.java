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

package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.net.NetworkScheduler;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:MemberName"})
public abstract class UringNetworkScheduler
        implements NetworkScheduler<UringAsyncSocket> {

    protected final UringAsyncServerSocket.Handler[] serverSockets_handlers;
    protected final int[] serverSockets_freeHandlers;
    protected int serverSockets_freeHandlersIndex;

    protected final UringAsyncSocket.Handler[] sockets_handlers;
    protected final int[] sockets_freeHandlers;
    protected int sockets_freeHandlersIndex;

    public UringNetworkScheduler(int socketLimit,
                                 int serverSocketLimit) {
        this.serverSockets_handlers = new UringAsyncServerSocket.Handler[socketLimit];
        this.serverSockets_freeHandlers = new int[serverSockets_handlers.length];
        for (int k = 0; k < serverSockets_handlers.length; k++) {
            serverSockets_freeHandlers[k] = k;
        }

        this.sockets_handlers = new UringAsyncSocket.Handler[serverSocketLimit];
        this.sockets_freeHandlers = new int[sockets_handlers.length];
        for (int k = 0; k < sockets_handlers.length; k++) {
            sockets_freeHandlers[k] = k;
        }
    }

    public void register(UringAsyncServerSocket.Handler handler) {
        checkNotNull(handler, "handler");

        int index = serverSockets_freeHandlers[serverSockets_freeHandlersIndex];
        serverSockets_freeHandlersIndex++;
        serverSockets_handlers[index] = handler;
        handler.handlerIndex = index;
    }

    public void unregister(UringAsyncServerSocket.Handler handler) {
        checkNotNull(handler, "handler");

        serverSockets_handlers[handler.handlerIndex] = null;
        serverSockets_freeHandlersIndex--;
        serverSockets_freeHandlers[serverSockets_freeHandlersIndex] = handler.handlerIndex;
    }

    public void register(UringAsyncSocket.Handler handler) {
        checkNotNull(handler, "handler");

        int index = sockets_freeHandlers[sockets_freeHandlersIndex];
        sockets_freeHandlersIndex++;
        sockets_handlers[index] = handler;
        handler.handlerIndex = index;
    }

    public void unregister(UringAsyncSocket.Handler handler) {
        checkNotNull(handler, "handler");

        sockets_handlers[handler.handlerIndex] = null;
        sockets_freeHandlersIndex--;
        sockets_freeHandlers[sockets_freeHandlersIndex] = handler.handlerIndex;
    }
}
