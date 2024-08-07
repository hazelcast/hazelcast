/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.networking.nio.NioChannel;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

/**
 * A Channel is a construct that can send/receive like Packets/ClientMessages etc.
 * Connections use a channel to do the real work; but there is no dependency of the
 * com.hazelcast.internal.networking to a Connection (no cycles). This means that a
 * Channel can be used perfectly without Connection.
 * <p>
 * The standard channel implementation is the {@link NioChannel} that uses TCP in
 * combination with selectors to transport data. In the future also other channel
 * implementations could be added, e.g. UDP based.
 * <p>
 * Channel data is read using a {@link InboundHandler}. E.g. data from a socket
 * is received and needs to get processed. The {@link InboundHandler} can convert
 * this to e.g. a Packet.
 * <p>
 * Channel data is written using a {@link OutboundHandler}. E.g. a packet needs
 * to be converted to bytes.
 * <p>
 * A Channel gets initialized using the {@link ChannelInitializer}.
 *
 * <h1>Future note</h1>
 * Below you can find some notes about the future of the Channel. This will hopefully
 * act as a guide how to move forward.
 *
 * <h2>Fragmentation</h2>
 * Packets are currently not fragmented, meaning that they are sent as 1 atomic unit
 * and this can cause a 2 way communication blackout for operations (since either
 * operation or response can get blocked). Fragmentation needs to be added to make sure
 * the system doesn't suffer from head of line blocking.
 *
 * <h2>Ordering</h2>
 * In a channel messages don't need to remain ordered. Currently, they are, but as soon as
 * we add packet fragmentation, packets can get out of order. Under certain conditions
 * you want to keep ordering, e.g. for events. In this case it should be possible
 * to have multiple streams in a channel. Within a stream there will be ordering.
 *
 * <h2>Reliability</h2>
 * A channel doesn't provide reliability. TCP/IP does provide reliability, but 1: who
 * says we want to keep using TCP/IP, but if a TCP/IP connection is lost and needs
 * to be re-established, packets are lost. Reliability can be added, just like TCP/IP
 * adds it; we don't discard the data until it has been acknowledged.
 *
 * <h2>Flow and congestion control</h2>
 * On the Channel level we have no flow of congestion control; frames are always accepted
 * no matter if on the sending side the write-queue is overloaded, or on the receiving side
 * the system is overloaded (e.g. many pending operations on the operation queue). Just like
 * with TCP/IP, flow and congestion control should be added.
 *
 * <h2>UDP</h2>
 * With ordering, reliability and flow and congestion control in place, we can ask
 * ourselves the question if UDP is not a more practical solution.
 */
public interface Channel extends Closeable {

    /**
     * Returns the {@link ChannelOptions} of this Channel.
     * <p>
     * Call is thread-safe; but calls to the ChannelOptions are not.
     *
     * @return the config for this channel. Returned value will never be null.
     */
    ChannelOptions options();

    /**
     * Returns the attribute map.
     * <p>
     * Attribute map can be used to store data into a socket. For example to find the
     * Connection for a Channel, one can store the Connection in this channel using some
     * well known key.
     *
     * @return the attribute map.
     */
    ConcurrentMap attributeMap();

    /**
     * Returns the {@link InboundPipeline} that belongs to this Channel.
     * <p>
     * This method is thread-safe, but most methods on the {@link InboundPipeline} are not!
     *
     * @return the InboundPipeline.
     */
    InboundPipeline inboundPipeline();

    /**
     * Returns the {@link OutboundPipeline} that belongs to this Channel.
     * <p>
     * This method is thread-safe, but most methods on the {@link OutboundPipeline} are not!
     *
     * @return the OutboundPipeline.
     */
    OutboundPipeline outboundPipeline();

    /**
     * @see java.nio.channels.SocketChannel#socket()
     *
     * This method will be removed from the interface. Only an explicit cast to NioChannel
     * will expose the Socket.
     * <p>
     * It is very important that the socket isn't closed directly; but one goes through the
     * {@link #close()} method so that interal administration of the channel is in sync with
     * that of the socket.
     */
    Socket socket();

    /**
     * @return the remote address. Returned value could be null.
     */
    SocketAddress remoteSocketAddress();

    /**
     * @return the local address. Returned value could be null
     */
    SocketAddress localSocketAddress();

    /**
     * Returns the last time epoch time in ms a read of the socket was done.
     * <p>
     * This method is thread-safe.
     *
     * @return the last time a read from the socket was done.
     */
    long lastReadTimeMillis();

    /**
     * Returns the last epoch time in ms that a write to the socket was done.
     * <p>
     * Writing to the socket doesn't mean that data has been sent or received; it means
     * that data was written to the SocketChannel. It could very well be that this data
     * is stuck somewhere in a network-buffer.
     * <p>
     * This method is thread-safe.
     *
     * @return the last time something was written to the socket.
     */
    long lastWriteTimeMillis();

    /**
     * Starts the Channel.
     * <p>
     * In case of a client-mode channel, the {@link #connect(InetSocketAddress, int)}
     * should be called before start is called.
     * <p>
     * When the Channel is started, the {@link ChannelInitializer} will be called to
     * initialize the channel and to start with processing inbound and outbound data.
     * <p>
     * This method is not thread-safe and should be made only once.
     * <p>
     * This method should be called for clientMode and non clientMode channels.
     * Otherwise, the connection will not start to read from or write to the socket.
     */
    void start();

    /**
     * Connects the channel.
     * <p>
     * This call should only be made once and is not thread-safe.
     * <p>
     * This call blocks until:
     * <ol>
     * <li>the connect succeeds</li>
     * <li>the connect fails</li>
     * <li>if there is a timeout.</li>
     * </ol>
     *
     * In case of failure, channel is closed automatically and exception is thrown to user.
     *
     * @param address       the address to connect to.
     * @param timeoutMillis the timeout in millis, or 0 if waiting indefinitely.
     * @throws IllegalStateException if the connection is not in clientMode.
     * @throws IOException if connecting fails.
     */
    void connect(InetSocketAddress address, int timeoutMillis) throws IOException;

    /**
     * Closes the Channel.
     * <p>
     * This method is thread-safe.
     * <p>
     * This method can safely be called from an IO thread. Close-listeners will not
     * be executed on an IO thread.
     * <p>
     * When the channel already is closed, the call is ignored.
     */
    @Override
    void close() throws IOException;

    /**
     * Checks if this Channel is closed. This method is very cheap to make.
     *
     * @return true if closed, false otherwise.
     */
    boolean isClosed();

    /**
     * Adds a ChannelCloseListener.
     * <p>
     * This method is thread-safe.
     *
     * @param listener the listener to register.
     * @throws NullPointerException if listener is null.
     */
    void addCloseListener(ChannelCloseListener listener);

    /**
     * Checks if this side is the Channel is in client mode or server mode.
     * <p>
     * A channel is in client-mode if it initiated the connection, and in
     * server-mode if it was the one accepting the connection. Client mode isn't related
     * to Hazelcast clients (although a Hazelcast client will always have clientMode=true).
     * A connection from one member to another member can also have clientMode=true if that
     * member connected to the other member.
     * <p>
     * One of the reasons this property is valuable is for protocol/handshaking
     * so that it is clear distinction between the side that initiated the connection,
     * or accepted the connection.
     * <p>
     * This method is thread-safe.
     *
     * @return true if this channel is in client-mode, false when in server-mode.
     * @see SSLEngine#getUseClientMode()
     */
    boolean isClientMode();

    /**
     * Queues the {@link OutboundFrame} to be written at some point in the future.
     * No guarantee is made that the frame actually is going to be written or received.
     * <p>
     * This method is thread-safe.
     *
     * @param frame the frame to write.
     * @return true if the frame was queued; false if rejected.
     */
    boolean write(OutboundFrame frame);

    /**
     * Returns current count of bytes read from the Channel.
     * The read values might not reflect the most recent value.
     *
     * @return count of read bytes
     */
    long bytesRead();

    /**
     * Returns current count of bytes written into the Channel.
     * The read values might not reflect the most recent value.
     *
     * @return count of written bytes
     */
    long bytesWritten();
}
