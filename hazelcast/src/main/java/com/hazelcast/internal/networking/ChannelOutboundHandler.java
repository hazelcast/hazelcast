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

package com.hazelcast.internal.networking;

import java.nio.ByteBuffer;

/**
 * Responsible for writing {@link OutboundFrame} to a {@link ByteBuffer}. For example a Packet needs to be written to a socket,
 * then it is taken from the queue of pending packets, the ChannelOutboundHandler is called with the Packet and the socket buffer
 * as argument and will then write the content of the packet to the buffer. And on completion, the content of the buffer is
 * written to the socket.
 *
 * {@link ChannelOutboundHandler} are not expected to be thread-safe; each channel will gets its own instance(s).
 *
 * For more information about the ChannelOutboundHandler (and handlers in generally), have a look at the
 * {@link ChannelInboundHandler}.
 *
 * If the main task of a ChannelOutboundHandler is to encode a message (e.g. a Packet), it is best to call this handler
 * an encoder. For example PacketEncoder.
 *
 * @param <F>
 * @see EventLoopGroup
 * @see ChannelInboundHandler
 * @see ChannelErrorHandler
 * @see Channel
 */
public abstract class ChannelOutboundHandler<F extends OutboundFrame> {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public ByteBuffer dst;
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public F frame;
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public ChannelOutboundHandler next;
    public ChannelOutboundHandler prev;

    public Channel channel;

    public final void setNext(ChannelOutboundHandler next) {
        this.next = next;
        onSetNext();
    }

    // todo: this method is problematic because we don't want to replace the last handler since that is the socket writer.
    public void appendLast(ChannelOutboundHandler last) {
        if (next == null) {
            last.prev = this;
            next = last;
        } else {
            next.appendLast(last);
        }
    }

    public void onSetNext() {
    }

    /**
     * Replaces the current handler.
     *
     * This method should be used when a handler should be used temporarily, e.g. to deal with protocol or TLS handshaking,
     * but once its task is complete, it can be replaced by a new handler. In case of protocol; once the protocol is known
     * the system could decide to e.g. set up a PacketEncoder as handler.
     *
     * @param handler
     */
    public void replaceBy(ChannelOutboundHandler handler) {
        // here we remove the protocol handler and replace it by the next handler in line (the MemberChannelOutboundHandler)
        ChannelOutboundHandler prev = this.prev;
        ChannelOutboundHandler next = this.next;

        handler.prev = prev;
        handler.next = next;
        handler.frame = frame;
        handler.channel = channel;

        prev.next = handler;
        if (next != null) {
            next.prev = handler;
        }

        // we need to set next because the handlers can be iterated over and the iterator could still be holding on to the old handler.
        this.next = handler;
    }

    /**
     * @throws Exception if something fails while writing to ByteBuffer. When an exception is thrown, the
     *                   {@link ChannelErrorHandler} is called.
     */
    public abstract WriteResult onWrite() throws Exception;

//
//    public void onWriteDelayed() throws Exception{
//        if(next !=null){
//            next.onWriteDelayed();
//        }
//    }
}
