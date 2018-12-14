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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.util.function.Supplier;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;

/**
 * A {@link OutboundHandler} for the new-client. It writes ClientMessages to the ByteBuffer.
 */
public class ClientMessageEncoder extends OutboundHandler<Supplier<ClientMessage>, ByteBuffer> {

    private ClientMessage message;

    @Override
    public HandlerStatus onWrite() {
        int batchedItemsWritten = 0;
        System.out.println(src);

        compactOrClear(dst);
        try {
            for (; ; ) {
                if (message == null) {
                    message = src.get();

                    if (message == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                if (message.writeTo(dst)) {
                    // message got written, lets see if another message can be written
                    batchedItemsWritten++;
                    message = null;
                } else {
                    // the message didn't get written completely, so we are done.
                    return DIRTY;
                }
            }
        } finally {
            dst.flip();
            System.out.println(channel+" ClientMessageEncoder, batched items written:"+batchedItemsWritten);
        }
    }
}
