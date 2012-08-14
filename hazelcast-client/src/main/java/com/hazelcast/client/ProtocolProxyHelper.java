/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.nio.ByteBuffer;
import java.util.logging.Level;

public class ProtocolProxyHelper extends ProxyHelper {
    private final String name;

    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());

    public ProtocolProxyHelper(String name, HazelcastClient client) {
        super(client);
        this.name = name;
    }

    public Object doCommand(Command command, String[] args, Data... data) {
        ByteBuffer[] buffers = new ByteBuffer[data == null ? 0 : data.length];
        int index = 0;
        for (Data d : data) {
            buffers[index++] = ByteBuffer.wrap(d.buffer);
        }
        long id = newCallId();
        Protocol protocol = new Protocol(null, command, String.valueOf(id), getCurrentThreadId(), false, args, buffers);
        Call call = new Call(id, protocol) {
            @Override
            public void onDisconnect(Member member) {
                if (!client.getOutRunnable().queue.contains(this)) {
                    logger.log(Level.FINEST, "Re enqueue " + this);
                    client.getOutRunnable().enQueue(this);
                }
            }
        };
        Object response = doCall(call);
        return response;
    }
}
