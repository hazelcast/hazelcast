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

import com.hazelcast.internal.tpcengine.net.NetworkScheduler;
import com.hazelcast.internal.tpcengine.util.CircularQueue;

import java.util.Queue;

/**
 * The {@link NetworkScheduler} specific to the {@link NioReactor}. It will
 * process sockets in FIFO order.
 * <p>
 * todo: The only thing this controls is the writing of data to the socket;
 * it doesn't control for example reading from the socket. So it controls
 * only a portion.
 */
public final class NioFifoNetworkScheduler implements NetworkScheduler<NioAsyncSocket> {

    private final CircularQueue<NioAsyncSocket> stagingQueue;

    public NioFifoNetworkScheduler(int socketLimit) {
        this.stagingQueue = new CircularQueue<>(socketLimit);
    }

    @Override
    public void scheduleWrite(NioAsyncSocket socket) {
        if (!stagingQueue.offer(socket)) {
            throw new IllegalStateException("Socket limit has been exceeded.");
        }
    }

    @Override
    public boolean tick() {
        boolean result = false;
        Queue<NioAsyncSocket> stagingQueue = this.stagingQueue;
        for (; ; ) {
            NioAsyncSocket socket = stagingQueue.poll();
            if (socket == null) {
                break;
            }

            socket.handler.run();
            result = true;
        }

        return result;
    }

    @Override
    public boolean hasPending() {
        return !stagingQueue.isEmpty();
    }
}
