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

package com.hazelcast.nio.ascii;

import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.annotation.PrivateApi;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@PrivateApi
public class TextEncoder implements ChannelOutboundHandler<TextCommand> {
    private final TcpIpConnection connection;
    private final Map<Long, TextCommand> responses = new ConcurrentHashMap<Long, TextCommand>(100);
    private long currentRequestId;

    public TextEncoder(TcpIpConnection connection) {
        this.connection = connection;
    }

    public void enqueue(TextCommand response) {
        long requestId = response.getRequestId();
        if (requestId == -1) {
            connection.write(response);
        } else {
            if (currentRequestId == requestId) {
                connection.write(response);
                currentRequestId++;
                processWaitingResponses();
            } else {
                responses.put(requestId, response);
            }
        }
    }

    private void processWaitingResponses() {
        TextCommand response = responses.remove(currentRequestId);
        while (response != null) {
            connection.write(response);
            currentRequestId++;
            response = responses.remove(currentRequestId);
        }
    }

    @Override
    public boolean onWrite(TextCommand textCommand, ByteBuffer dst) throws Exception {
        return textCommand.writeTo(dst);
    }
}
