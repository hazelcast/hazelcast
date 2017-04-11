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

import com.hazelcast.nio.tcp.MemberReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.nio.ByteBuffer;

/**
 * Reads content from a {@link ByteBuffer} and processes it. The ReadHandler is invoked by the {@link SocketReader} after
 * it has read data from the socket.
 *
 * A typical example is that Packet instances are created from the buffered data and handing them over the the
 * {@link com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher}. See {@link MemberReadHandler} for more information.
 *
 * Each {@link SocketReader} will have its own {@link ReadHandler} instance. Therefor it doesn't need to be thread-safe.
 *
 * <h1>Pipelining & Encryption</h1>
 * The ReadHandler/WriteHandler can also form a pipeline. For example for SSL there could be a initial ReadHandler that decryption
 * the ByteBuffer and passes the decrypted ByteBuffer to the next ReadHandler; which could be a {@link MemberReadHandler}
 * that reads out any Packet from the decrypted ByteBuffer. Using this approach encryption can easily be added to any type of
 * communication, not only member 2 member communication.
 *
 * Currently security is added by using a {@link SocketChannelWrapper}, but this is not needed if the handlers form a pipeline.
 * Netty follows a similar approach with pipelining and adding encryption.
 *
 * There is no explicit support for setting up a 'pipeline' of ReadHandler/WriterHandlers but t can easily be realized by setting
 * up the chain and let a handler explicitly forward to the next. Since it isn't a common practice for the handler so far, isn't
 * needed to add additional complexity to the system; just set up a chain manually.
 *
 * pseudo code:
 * <pre>
 *     public class DecryptingReadHandler implements ReadHandler {
 *         private final ReadHandler next;
 *
 *         public DecryptingReadHandler(ReadHandler next) {
 *             this.next = next;
 *         }
 *
 *         public void onRead(ByteBuffer src) {
 *             decrypt(src, decryptedSrc);
 *             next.onRead(decryptedSrc)
 *         }
 *     }
 * </pre>
 * The <code>next</code> ReadHandler is the next item in the pipeline.
 *
 * For encryption is similar approach can be followed where the DecryptingWriteHandler is the last WriteHandler in the pipeline.
 *
 * @see WriteHandler
 * @see SocketReader
 * @see TcpIpConnection
 * @see IOThreadingModel
 */
public interface ReadHandler {

    /**
     * A callback to indicate that data is available in the ByteBuffer to be processed.
     *
     * @param src the ByteBuffer containing the data to read. The ByteBuffer is already in reading mode and when completed,
     *            should not be converted to write-mode using clear/compact. That is a task of the {@link SocketReader}.
     * @throws Exception if something fails while reading data from the ByteBuffer or processing the data
     *                   (e.g. when a Packet fails to get processed). When an exception is thrown, the TcpIpConnection
     *                   is closed. There is no point continuing with a potentially corrupted stream.
     */
    void onRead(ByteBuffer src) throws Exception;
}
