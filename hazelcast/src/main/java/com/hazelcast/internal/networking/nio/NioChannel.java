/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.AbstractChannel;
import com.hazelcast.internal.networking.OutboundFrame;

import java.nio.channels.SocketChannel;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * A {@link com.hazelcast.internal.networking.Channel} implementation tailored
 * for non blocking IO using {@link java.nio.channels.Selector} in combination
 * with a non blocking {@link SocketChannel}.
 */
public class NioChannel extends AbstractChannel {

    NioInboundPipeline inboundPipeline;
    NioOutboundPipeline outboundPipeline;

    public NioChannel(SocketChannel socketChannel, boolean clientMode) {
        super(socketChannel, clientMode);
    }

    public void init(NioInboundPipeline inboundPipeline, NioOutboundPipeline outboundPipeline) {
        this.inboundPipeline = inboundPipeline;
        this.outboundPipeline = outboundPipeline;
    }

    public NioOutboundPipeline outboundPipeline() {
        return outboundPipeline;
    }

    public NioInboundPipeline inboundPipeline() {
        return inboundPipeline;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (isClosed()) {
            return false;
        }
        outboundPipeline.write(frame);
        return true;
    }

    @Override
    public long lastReadTimeMillis() {
        return inboundPipeline.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return outboundPipeline.lastWriteTimeMillis();
    }

    @Override
    public void flush() {
        outboundPipeline.flush();
    }

    @Override
    protected void onClose() {
        closeResource(inboundPipeline);
        closeResource(outboundPipeline);
    }

    @Override
    public String toString() {
        return "NioChannel{" + localSocketAddress() + "->" + remoteSocketAddress() + '}';
    }
}
