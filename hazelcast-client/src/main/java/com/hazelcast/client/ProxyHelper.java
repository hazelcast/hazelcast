/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyHelper {

    private final static AtomicLong callIdGen = new AtomicLong(0);
    protected final HazelcastClient client;
    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());
    ProtocolWriter writer = new ProtocolWriter();
    ProtocolReader reader = new ProtocolReader();
    ConnectionPool connectionPool;

    public ProxyHelper(String a, HazelcastClient client) {
        this.client = client;
        connectionPool = new ConnectionPool(client.getConnectionManager());
    }

    public int getCurrentThreadId() {
        return (int) Thread.currentThread().getId();
    }

    public static Long newCallId() {
        return callIdGen.incrementAndGet();
    }

    public void destroy() {
//        doOp(ClusterOperation.DESTROY, null, null);
//        this.client.destroy(name);
    }

    public <K> Collection<K> keys(Predicate predicate) {
//        Keys keys = (Keys) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, predicate);
        Collection<K> collection = new ArrayList<K>();
//        for (Data d : keys) {
//            collection.add((K) toObject(d.buffer));
//        }
        return collection;
    }

    public <K> Collection<K> entries(final Predicate predicate) {
//        Keys keys = (Keys) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, null, predicate);
        Collection<K> collection = new ArrayList<K>();
//        for (Data d : keys) {
//            collection.add((K) toObject(d.buffer));
//        }
        return collection;
    }

    public static void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
//        if (!(obj instanceof Serializable)) {
//            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
//        }
    }

    public static void checkTime(long time, TimeUnit timeunit) {
        if (time < 0) {
            throw new IllegalArgumentException("Time can not be less than 0.");
        }
        if (timeunit == null) {
            throw new NullPointerException("TimeUnit can not be null.");
        }
    }

    public static void check(EventListener listener) {
        if (listener == null) {
            throw new NullPointerException("Listener can not be null");
        }
    }

    public Data toData(Object obj) {
        return client.getSerializationService().toData(obj);
    }

    public Object toObject(Data data) {
        return client.getSerializationService().toObject(data);
    }

    private Object getSingleObjectFromResponse(Protocol response) {
        if (response!=null && response.buffers != null && response.hasBuffer()) {
            return client.getSerializationService().toObject(
                    new Data(SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY, response.buffers[0].array()));
        } else return null;
    }

    public void doFireNForget(Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command,args, data);
        try {
            Connection connection = connectionPool.takeConnection();
            writer.write(connection,protocol);
            connectionPool.releaseConnection(connection);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
        }
    }

    public Protocol doCommand(Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command,args, data);
        try {
            Connection connection = connectionPool.takeConnection();
            protocol.onEnqueue();
            writer.write(connection,protocol);
            writer.flush(connection);
            Protocol response = reader.read(connection);
            connectionPool.releaseConnection(connection);
            return response;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Protocol createProtocol(Command command, String[] args, Data[] data) {
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
        return protocol;
    }

    <V> Future<V> doAsync(final Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command, args, data);
        try {
            final Connection connection = connectionPool.takeConnection();
            writer.write(connection, protocol);
            return new Future<V>() {
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                public boolean isCancelled() {
                    return false;
                }

                public boolean isDone() {
                    return false;
                }

                public V get() throws InterruptedException, ExecutionException {
                    Protocol protocol = null;
                    try {
                        protocol = reader.read(connection);
                    } catch (IOException e) {
                        throw new ExecutionException(e);
                    }
                    return (V)ProxyHelper.this.getSingleObjectFromResponse(protocol);
                }

                public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    Protocol protocol = null;
                    try {
                        protocol = reader.read(connection, timeout, unit);
                    } catch (IOException e) {
                        throw new ExecutionException(e);
                    }
                    return (V)ProxyHelper.this.getSingleObjectFromResponse(protocol);
                }
            };
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
        Protocol response = doCommand(command, new String[]{arg}, data);
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
        Protocol response = doCommand(command, arg, data);
        return getSingleObjectFromResponse(response);
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
