/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.networking.WriteHandler;

import java.nio.ByteBuffer;

/**
 * A {@link WriteHandler} for the new-client. It writes ClientMessages to the ByteBuffer.
 *
 * @see ClientReadHandler
 */
public class ClientWriteHandler implements WriteHandler<ClientMessage> {

    @Override
    public boolean onWrite(ClientMessage message, ByteBuffer dst) throws Exception {
        return message.writeTo(dst);
    }
}
