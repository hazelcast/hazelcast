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

public class IOUringNetworkScheduler implements NetworkScheduler<IOUringAsyncSocket> {
    private final Queue<IOUringAsyncSocket> dirtyQueue;

    public IOUringNetworkScheduler(int maxSockets) {
        this.dirtyQueue = new MpscArrayQueue<>(maxSockets);
    }

    @Override
    public boolean schedule(IOUringAsyncSocket socket) {
        return dirtyQueue.offer(socket);
    }

    public void tick() {
        for (; ; ) {
            IOUringAsyncSocket socket = dirtyQueue.poll();
            if (socket == null) {
                break;
            }

            socket.submissionHandler_write.run();
        }
    }

    @Override
    public boolean hasPending() {
        return !dirtyQueue.isEmpty();
    }
}
