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

package com.hazelcast.internal.networking;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

/**
 * A Channel is a construct that can send/receive like Packets/ClientMessages etc. Connections use a channel to do the real
 * work; but there is no dependency of the com.hazelcast.internal.networking to a Connection (no cycles). This means that a
 * Channel can be used perfectly without Connection.
 *
 * The standard channel implementation is the {@link com.hazelcast.internal.networking.nio.NioChannel} that uses TCP in
 * combination with selectors to transport data. In the future also other channel implementations could be added, e.g.
 * UDP based.
 *
 * Channel data is read using a {@link ChannelInboundHandler}. E.g data from a socket is received and needs to get processed.
 * The {@link ChannelInboundHandler} can convert this to e.g. a Packet.
 *
 * Channel data is written using a {@link ChannelOutboundHandler}. E.g. a packet needs to be converted to bytes.
 *
 * A Channel gets initialized using the {@link ChannelInitializer}.
 *
 * <h1>Future note</h1>
 * Below you can find some notes about the future of the Channel. This will hopefully act as a guide how to move forward.
 *
 * <h2>Fragmentation</h2>
 * Packets are currently not fragmented, meaning that they are send as 1 atomic unit and this can cause a 2 way communication
 * blackout for operations (since either operation or response can get blocked). Fragmentation needs to be added to make sure
 * the system doesn't suffer from head of line blocking.
 *
 * <h2>Ordering</h2>
 * In a channel messages don't need to be ordered. Currently they are, but as soon as we add packet fragmentation, packets
 * can get out of order. Under certain conditions you want to keep ordering, e.g. for events. In this case it should be possible
 * to have multiple streams in a channel. Within a stream there will be ordering.
 *
 * <h2>Reliability</h2>
 * A channel doesn't provide reliability. TCP/IP does provide reliability, but 1: who says we want to keep using TCP/IP, but
 * if a TCP/IP connection is lost and needs to be re-established, packets are lost. Reliability can be added, just like TCP/IP
 * adds it; we don't discard the data until it has been acknowledged.
 *
 * <h2>Flow and congestion control</h2>
 * On the Channel level we have no flow of congestion control; frames are always accepted no matter if on the sending side
 * the write-queue is overloaded, or on the receiving side the system is overloaded (e.g. many pending operations on the
 * operation queue). Just like with TCP/IP, flow and congestion control should be added.
 *
 * <h2>UDP</h2>
 * With ordering, reliability and flow and congestion control in place, we can ask ourselves the question if UDP is not a
 * more practical solution.
 */
public interface Channel extends Closeable {

    /**
     * Returns the attribute map.
     *
     * Attribute map can be used to store data into a socket. For example to find the Connection for a Channel, one can
     * store the Connection in this channel using some well known key.
     *
     * @return the attribute map.
     */
    ConcurrentMap attributeMap();

    /**
     * @see java.nio.channels.SocketChannel#socket()
     *
     * This method will be removed from the interface. Only an explicit cast to NioChannel will expose the Socket.
     *
     * It is very important that the socket isn't closed directly; but one goes through the {@link #close()} method so that
     * interal administration of the channel is in sync with that of the socket.
     */
    Socket socket();

    /**
     * @return the remote address. Returned value could be null.
     */
    SocketAddress getRemoteSocketAddress();

    /**
     * @return the local address. Returned value could be null
     */
    SocketAddress getLocalSocketAddress();

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} a read of the socket was done.
     *
     * @return the last time a read from the socket was done.
     */
    long lastReadTimeMillis();

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} that a write to the socket completed.
     *
     * Writing to the socket doesn't mean that data has been send or received; it means that data was written to the
     * SocketChannel. It could very well be that this data is stuck somewhere in an io-buffer.
     *
     * @return the last time something was written to the socket.
     */
    long lastWriteTimeMillis();

    /**
     * This method will be removed from the Channel in the near future.
     *
     * @see java.nio.channels.SocketChannel#read(ByteBuffer)
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * This method will be removed from the Channel in the near future.
     *
     * @see java.nio.channels.SocketChannel#write(ByteBuffer)
     */
    int write(ByteBuffer src) throws IOException;

    /**
     * Closes inbound.
     *
     * <p>Not thread safe. Should be called in channel reader thread.</p>
     *
     * Method will be removed once the TLS integration doesn't rely on subclassing this channel.
     *
     * @throws IOException
     */
    void closeInbound() throws IOException;

    /**
     * Closes outbound.
     *
     * <p>Not thread safe. Should be called in channel writer thread.</p>
     *
     * Method will be removed once the TLS integration doesn't rely on subclassing this channel.
     *
     * @throws IOException
     */
    void closeOutbound() throws IOException;

    /**
     * Closes the Channel.
     *
     * When the channel already is closed, the call is ignored.
     */
    void close() throws IOException;

    /**
     * Checks if this Channel is closed. This method is very cheap to make.
     *
     * @return true if closed, false otherwise.
     */
    boolean isClosed();

    /**
     * Adds a ChannelCloseListener.
     *
     * @param listener the listener to register.
     * @throws NullPointerException if listener is null.
     */
    void addCloseListener(ChannelCloseListener listener);

    /**
     * Checks if this side is the Channel is in client mode or server mode.
     *
     * A channel is in client-mode if it initiated the connection, and in server-mode if it was the one accepting the connection.
     *
     * One of the reasons this property is valuable is for protocol/handshaking so that it is clear distinction between the
     * side that initiated the connection, or accepted the connection.
     *
     * @return true if this channel is in client-mode, false when in server-mode.
     * @see SSLEngine#getUseClientMode()
     */
    boolean isClientMode();

    /**
     * Queues the {@link OutboundFrame} to be written at some point in the future. No guarantee is made that the
     * frame actually is going to be written or received.
     *
     * @param frame the frame to write.
     * @return true if the frame was queued; false if rejected.
     */
    boolean write(OutboundFrame frame);

    /**
     * Flushes whatever needs to be written.
     *
     * Normally this call doesn't need to be made since {@link #write(OutboundFrame)} write the content; but in case of protocol
     * and TLS handshaking, such triggers are needed.
     */
    void flush();
}
