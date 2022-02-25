/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * A {@link OutboundHandler} that writes the client protocol bytes
 * and once they have been fully written, it removes itself from pipeline.
 *
 * On a plain connection, this should be the last encoder in the pipeline.
 *
 * Even though the ClientProtocolEncoder has a ByteBuffer as src, it will
 * never consume bytes from this source.
 */
public class ClientProtocolEncoder extends OutboundHandler<ByteBuffer, ByteBuffer> {

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH, stringToBytes(CLIENT_BINARY));
    }

    @Override
    public HandlerStatus onWrite() {
        if (dst.remaining() == 0) {
            channel.outboundPipeline().remove(this);
            return CLEAN;
        } else {
            return DIRTY;
        }
    }
}
