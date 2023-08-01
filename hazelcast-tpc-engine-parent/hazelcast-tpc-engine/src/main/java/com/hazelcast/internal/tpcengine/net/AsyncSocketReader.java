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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The {@link AsyncSocketReader} is called when data is received on an
 * {@link AsyncSocket} and needs to be processed by the application. This
 * is where e.g. ClientMessages of Packets could be created.
 */
public abstract class AsyncSocketReader {

    protected AsyncSocket socket;
    protected Reactor reactor;
    protected Eventloop eventloop;

    /**
     * Initializes the ReadHandler. This method is called once and from the
     * eventloop thread that owns the socket.
     *
     * @param socket the socket this ReadHandler belongs to.
     */
    public void init(AsyncSocket socket) {
        this.socket = checkNotNull(socket);
        this.reactor = socket.reactor();
        this.eventloop = reactor.eventloop();
    }

    /**
     * Process the received data on the socket.
     * <p/>
     * Idea:
     * Currently we are forced to consume the bytes from the src buffer; so we need to copy them into
     * a different structure (e.g. an IOBuffer) because the src buffer will be used for the next
     * read from the socket. This can be solved by letting this method return the next 'src' buffer;
     * so the buffer where the data from the socket is going to end up in. This way the current 'src'
     * buffer can be used for processing, while the returned src buffer can be used to read the new data
     * from the socket from.
     *
     * @param src the ByteBuffer containing the received data.
     */
    public abstract void onRead(ByteBuffer src);
}
