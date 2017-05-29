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

import com.hazelcast.internal.networking.AbstractChannel;
import com.hazelcast.internal.networking.OutboundFrame;

import java.nio.channels.SocketChannel;

public class SpinningChannel extends AbstractChannel {

    private SpinningChannelReader reader;
    private SpinningChannelWriter writer;

    public SpinningChannel(SocketChannel socketChannel, boolean clientMode) {
        super(socketChannel, clientMode);
    }

    public void setReader(SpinningChannelReader reader) {
        this.reader = reader;
    }

    public void setWriter(SpinningChannelWriter writer) {
        this.writer = writer;
    }

    public SpinningChannelReader getReader() {
        return reader;
    }

    public SpinningChannelWriter getWriter() {
        return writer;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (isClosed()) {
            return false;
        }
        writer.write(frame);
        return true;
    }

    @Override
    public void flush() {
        // no-op since the threads will continuously scan the reader/writers.
    }

    @Override
    public long lastReadTimeMillis() {
        return reader.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return writer.lastWriteTimeMillis();
    }

    @Override
    public String toString() {
        return "SpinningChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
    }
}
