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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.server.ServerConnection;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.JVMUtil.upcast;

public class TextEncoder extends OutboundHandler<Supplier<TextCommand>, ByteBuffer> {
    public static final String TEXT_ENCODER = "textencoder";

    private final ServerConnection connection;
    private final Map<Long, TextCommand> responses = new ConcurrentHashMap<Long, TextCommand>(100);
    private long currentRequestId;
    private TextCommand command;

    public TextEncoder(ServerConnection connection) {
        this.connection = connection;
    }

    @Override
    public void handlerAdded() {
        initDstBuffer();
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
    public HandlerStatus onWrite() {
        // the buffer is in reading mode

        compactOrClear(dst);
        try {
            for (; ; ) {
                if (command == null) {
                    command = src.get();

                    if (command == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                if (command.writeTo(dst)) {
                    // command got written, lets see if another command can be written
                    command = null;
                } else {
                    // the command didn't get written completely
                    return DIRTY;
                }
            }
        } finally {
            upcast(dst).flip();
        }
    }
}
