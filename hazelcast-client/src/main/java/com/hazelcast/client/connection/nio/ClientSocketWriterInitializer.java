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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.internal.networking.SocketWriterInitializer;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Protocols;

import java.nio.ByteBuffer;

class ClientSocketWriterInitializer implements SocketWriterInitializer<ClientConnection> {

    private final int bufferSize;
    private final boolean direct;

    ClientSocketWriterInitializer(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    @Override
    public void init(ClientConnection connection, SocketWriter writer, String protocol) {
        Logger.getLogger(getClass())
              .fine("Initializing ClientSocketWriter WriteHandler with " + Protocols.toUserFriendlyString(protocol));

        writer.initOutputBuffer(IOUtil.newByteBuffer(bufferSize, direct));

        writer.initWriteHandler(new WriteHandler<ClientMessage>() {
            @Override
            public boolean onWrite(ClientMessage msg, ByteBuffer dst) throws Exception {
                return msg.writeTo(dst);
            }
        });
    }
}
