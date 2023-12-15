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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;

import java.nio.ByteBuffer;
import java.util.Queue;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The {@link AsyncSocketWriter} is called to convert messages from the writeQueue to
 * bytes so they can be send over the socket.
 * <p/>
 * A writer is specific to a {@link AsyncSocket} and can't be shared between
 * multiple AsyncSocket instances.
 */
public abstract class AsyncSocketWriter {
    protected AsyncSocket socket;
    protected Reactor reactor;
    protected Eventloop eventloop;
    protected Queue writeQueue;

    /**
     * Initializes the Reader. This method is called once and from the
     * eventloop thread that owns the socket.
     *
     * @param socket the socket this Reader belongs to.
     */
    public void init(AsyncSocket socket, Queue writeQueue) {
        this.socket = checkNotNull(socket);
        this.writeQueue = writeQueue;
        this.reactor = socket.reactor();
        this.eventloop = reactor.eventloop();
    }

    /**
     * Is called when data needs to be written to the socket.
     *
     * @return true if the Writer is clean, false if dirty. It is dirty
     * when it could not manage to write all data to the dst buffer.
     */
    public abstract boolean onWrite(ByteBuffer dst);
}
