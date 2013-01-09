/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;


public class ProtocolProxyHelper extends ProxyHelper {
    private final String name;

    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());

    public ProtocolProxyHelper(String name, HazelcastClient client) {
        super(client);
        this.name = name;
    }

    /**
     * Returns true if the response is OK, otherwise returns false.
     *
     * @param command
     * @param arg
     * @param data
     * @return
     */
    public boolean doCommand(Command command, String arg, Data... data) {
        Protocol response = this.doCommand(command, new String[]{arg}, data);
        return response.command.equals(Command.OK);
    }

    /**
     * Expects one binary data on response and returns the deserialized version of that binary data.
     *
     * @param command
     * @param arg
     * @param data
     * @return
     */
    public Object doCommandAsObject(Command command, String[] arg, Data... data) {
        Protocol response = this.doCommand(command, arg, data);
        return getSingleObjectFromResponse(response);
    }

    private Object getSingleObjectFromResponse(Protocol response) {
        if (response!=null && response.buffers != null && response.hasBuffer()) {
            return client.getSerializationService().toObject(
                    new Data(SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY, response.buffers[0].array()));
        } else return null;
    }

    /**
     * Expects one binary data on response and returns the deserialized version of that binary data.
     *
     * @param command
     * @param arg
     * @param data
     * @return
     */
    public Object doCommandAsObject(Command command, String arg, Data... data) {
        return doCommandAsObject(command, new String[]{arg}, data);
    }

    public void doFireNForget(Command command, String[] args, Data... data) {
        Call call = createCall(command, args, data);
        call.setFireNforget(true);
        sendCall(call);
    }
    public Protocol doCommand(Command command, String[] args, Data... data) {
        Call call = createCall(command, args, data);
        Protocol response = (Protocol) doCall(call);
        return response;
    }

    private Call createCall(Command command, String[] args, Data[] data) {
        ByteBuffer[] buffers = new ByteBuffer[data == null ? 0 : data.length];
        int index = 0;
        if (data != null) {
            for (Data d : data) {
                if (d != null && d.buffer != null)
                    buffers[index++] = ByteBuffer.wrap(d.buffer);
            }
        }
        if (args == null) args = new String[]{};
        long id = newCallId();
        Protocol protocol = new Protocol(null, command, String.valueOf(id), getCurrentThreadId(), false, args, buffers);
        return new Call(id, protocol) {
            @Override
            public void onDisconnect(Member member) {
                if (!client.getOutRunnable().queue.contains(this)) {
                    logger.log(Level.FINEST, "Re enqueue " + this);
                    client.getOutRunnable().enQueue(this);
                }
            }
        };
    }

    <V> Future<V> doAsync(final Command command, String[] args, Data... data) {
        final Call call = createCall(command, args, data);
        sendCall(call);

        return new Future<V>() {
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            public boolean isCancelled() {
                return false;
            }

            public boolean isDone() {
                return call.hasResponse();
            }

            public V get() throws InterruptedException, ExecutionException {
                Protocol protocol =  (Protocol)ProtocolProxyHelper.this.getResponse(call);
                return (V)ProtocolProxyHelper.this.getSingleObjectFromResponse(protocol);
            }

            public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                Protocol protocol =  (Protocol)ProtocolProxyHelper.this.getResponse(call, timeout, unit);
                return (V)ProtocolProxyHelper.this.getSingleObjectFromResponse(protocol);
            }
        };
    }

    public boolean doCommandAsBoolean(Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(command, args, datas);
        return Boolean.valueOf(protocol.args[0]);
    }

    public int doCommandAsInt(Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(command, args, datas);
        return Integer.valueOf(protocol.args[0]);
    }
    
    public <E> Collection<E> doCommandAsList(Command command, String[] args, Data... datas){
        Protocol protocol = doCommand(command, args, datas);
        List<E> list = new ArrayList<E>();
        if(protocol.hasBuffer()){
            for(ByteBuffer bb: protocol.buffers){
                list.add((E) client.getSerializationService().toObject(
                        new Data(SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY, bb.array())));
            }
        }
        return list;
    }
}
