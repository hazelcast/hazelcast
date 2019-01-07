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

package com.hazelcast.client.connection.nio;

import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.util.StringUtil.stringToBytes;

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
    public HandlerStatus onWrite() {
        //System.out.println(channel+" ClientProtocolEncoder.onWrite "+ IOUtil.toDebugString("dst",dst));

        compactOrClear(dst);
        try {
            dst.put(stringToBytes(CLIENT_BINARY_NEW));
           // System.out.println(channel+" ClientProtocolEncoder protocol written");
            channel.outboundPipeline().remove(this);
            return CLEAN;
        } finally {
            dst.flip();
        }
    }
}
