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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpcengine.TaskFactory;
import com.hazelcast.internal.tpcengine.TaskQueue;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.FrameCodec.FLAG_PACKET;
import static com.hazelcast.internal.tpc.FrameCodec.FLAG_RES;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_UUID;

/**
 * The FrameDecoder decodes frames (which can contain a nested packet).
 */
@SuppressWarnings({"checkstyle:VisibilityModifier",
        "checkstyle:CyclomaticComplexity",
        "checkstyle:NPathComplexity"})
public class FrameDecoder extends AsyncSocketReader {
    private static final int STATE_INITIAL = 0;
    private static final int STATE_READING_NOTHING = 1;
    private static final int STATE_READING_PACKET = 2;
    private static final int STATE_READING_FRAME = 3;

    public IOBufferAllocator requestAllocator;
    public IOBufferAllocator responseAllocator;
    public TaskFactory taskFactory;
    public Consumer<IOBuffer> responseHandler;
    public ServerConnectionManager connectionManager;
    public TaskQueue taskGroup;

    private final PacketIOHelper packetIOHelper = new PacketIOHelper();
    private IOBuffer frame;
    private int state = STATE_INITIAL;
    private ServerConnection connection;

    @Override
    public void onRead(ByteBuffer src) {
        IOBuffer responseChain = null;
        for (; ; ) {
            if (state == STATE_INITIAL) {
                if (src.remaining() < SIZEOF_UUID) {
                    // not enough bytes available to read the uuid
                    break;
                }

                findConnection(src);
                state = STATE_READING_NOTHING;
            }

            if (state == STATE_READING_NOTHING) {
                if (src.remaining() < SIZEOF_INT + SIZEOF_INT) {
                    // not enough bytes are available.
                    break;
                }

                int size = src.getInt();
                int flags = src.getInt();
                if ((flags & FLAG_PACKET) == 0) {
                    state = STATE_READING_FRAME;
                    if ((flags & FLAG_RES) == 0) {
                        frame = requestAllocator.allocate(size);
                    } else {
                        frame = responseAllocator.allocate(size);
                    }
                    frame.byteBuffer().limit(size);
                    frame.writeInt(size);
                    frame.writeInt(flags);
                    frame.socket = socket;
                } else {
                    state = STATE_READING_PACKET;
                }
            }

            if (state == STATE_READING_FRAME) {
                int size = FrameCodec.size(frame);
                int remaining = size - frame.position();
                frame.write(src, remaining);

                if (!FrameCodec.isComplete(frame)) {
                    break;
                }

                frame.flip();
                //framesRead.inc();
                if (FrameCodec.isFlagRaised(frame, FLAG_RES)) {
                    frame.next = responseChain;
                    responseChain = frame;
                } else {
                    // todo: return
                    taskGroup.offerLocal(frame);
                    //taskFactory.schedule(frame);
                }
                frame = null;
            } else {
                Packet packet = packetIOHelper.readFrom(src);
                if (packet == null) {
                    // not enough data available to encode a packet
                    break;
                }
                packet.setConn(connection);
                // todo: return
                taskGroup.offerLocal(packet);
                //taskFactory.schedule(packet);
            }

            state = STATE_READING_NOTHING;
        }

        if (responseChain != null) {
            responseHandler.accept(responseChain);
        }
    }

    private void findConnection(ByteBuffer src) {
        UUID uuid = new UUID(src.getLong(), src.getLong());

        while (connection == null) {
            // Not very efficient. Worry for later.
            for (ServerConnection c : connectionManager.getConnections()) {
                if (uuid.equals(c.getRemoteUuid())) {
                    connection = c;
                    break;
                }
            }

            if (connection == null) {
                throw new RuntimeException("Connection with uuid " + uuid + " not found.");
            }
        }
    }
}
