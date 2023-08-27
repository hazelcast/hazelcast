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
import org.jctools.queues.MpscArrayQueue;

import java.util.Queue;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.decodeIndex;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.decodeOpcode;

/**
 * The {@link NetworkScheduler} for the {@link UringReactor}. Dirty sockets are
 * processed in FIFO order.
 */
public final class UringFifoNetworkScheduler
        extends UringNetworkScheduler {

    private final Queue<UringAsyncSocket> stagingQueue;

    public UringFifoNetworkScheduler(Uring uring,
                                     int socketLimit,
                                     int serverSocketLimit) {
        super(socketLimit, serverSocketLimit);
        this.stagingQueue = new MpscArrayQueue<>(socketLimit);

        uring.completionQueue().registerSocketHandler(this::completeSocket);
        uring.completionQueue().registerServerSocketHandler(this::completeServerSocket);
    }

    @Override
    public void schedule(UringAsyncSocket socket) {
        if (!stagingQueue.offer(socket)) {
            throw new IllegalStateException("Socket limit has been exceeded.");
        }
    }

    public void completeSocket(int res, int flags, long userdata) {
        int index = decodeIndex(userdata);
        byte opcode = decodeOpcode(userdata);
        sockets_handlers[index].complete(opcode, res);
    }

    public void completeServerSocket(int res, int flags, long userdata) {
        int index = decodeIndex(userdata);
        byte opcode = decodeOpcode(userdata);
        serverSockets_handlers[index].complete(opcode, res);
    }

    @Override
    public boolean tick() {
        for (; ; ) {
            UringAsyncSocket socket = stagingQueue.poll();
            if (socket == null) {
                break;
            }

            socket.handler.prepareWrite();
        }

        //todo
        return false;
    }

    @Override
    public boolean hasPending() {
        return !stagingQueue.isEmpty();
    }
}
