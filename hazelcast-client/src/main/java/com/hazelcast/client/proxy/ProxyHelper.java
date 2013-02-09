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

package com.hazelcast.client.proxy;

import com.hazelcast.client.*;
import com.hazelcast.client.proxy.listener.ListenerResponseHandler;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyHelper {

    private final static AtomicLong callIdGen = new AtomicLong(0);
    private final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());
    final ProtocolWriter writer;
    final ProtocolReader reader;
    final ConnectionPool cp;
    private final SerializationService ss;

    public ProxyHelper(SerializationService ss, ConnectionPool cp) {
        this.cp = cp;
        this.ss = ss;
        this.writer = new ProtocolWriter(ss);
        this.reader = new ProtocolReader(ss);
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
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
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
        return ss.toData(obj);
    }

    public Object toObject(Data data) {
        return ss.toObject(data);
    }

    private Object getSingleObjectFromResponse(Protocol response) {
        if (response != null && response.buffers != null && response.hasBuffer()) {
            return ss.toObject(response.buffers[0]);
        } else return null;
    }

    public void doFireNForget(Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command, args, data);
        try {
            protocol.onEnqueue();
            Connection connection = cp.takeConnection(null);
            writer.write(connection, protocol);
            cp.releaseConnection(connection, null);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
        }
    }

    public ListenerThread createAListenerThread(String threadName, HazelcastClient client, Protocol request, ListenerResponseHandler lrh) {
        InetSocketAddress isa = client.getCluster().getMembers().iterator().next().getInetSocketAddress();
        final Connection connection = client.getConnectionManager().createAndBindConnection(isa);
        ListenerThread thread = new ListenerThread(threadName, request, lrh, connection, ss);
        return thread;
    }

    public Protocol doCommand(Data key, Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command, args, data);
        try {
            protocol.onEnqueue();
            Context context = Context.get();
            Connection connection = context == null ? null : context.getConnection();
            if (connection == null)
                connection = cp.takeConnection(key);
            writer.write(connection, protocol);
            writer.flush(connection);
            Protocol response = reader.read(connection);
            cp.releaseConnection(connection, key);
            if (Command.OK.equals(response.command))
                return response;
            else {
                throw new RuntimeException(response.command + ": " + Arrays.asList(response.args));
            }
        } catch (EOFException e) {
            return doCommand(key, command, args, data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Protocol createProtocol(Command command, String[] args, Data[] data) {
        if (args == null) args = new String[]{};
        long id = newCallId();
        Protocol protocol = new Protocol(null, command, String.valueOf(id), getCurrentThreadId(), false, args, data);
        return protocol;
    }

    <V> Future<V> doAsync(final Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command, args, data);
        protocol.onEnqueue();
        try {
            final Connection connection = cp.takeConnection(null);
            writer.write(connection, protocol);
            writer.flush(connection);
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
                    return (V) ProxyHelper.this.getSingleObjectFromResponse(protocol);
                }

                public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    Protocol protocol = null;
                    try {
                        protocol = reader.read(connection, timeout, unit);
                    } catch (IOException e) {
                        throw new ExecutionException(e);
                    }
                    return (V) ProxyHelper.this.getSingleObjectFromResponse(protocol);
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
    public boolean doCommand(Data key, Command command, String arg, Data... data) {
        String[] args;
        if(arg == null)
            args = new String[]{};
        else 
            args = new String[]{arg};
        Protocol response = doCommand(key, command, args, data);
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
    public Object doCommandAsObject(Data key, Command command, String[] arg, Data... data) {
        Protocol response = doCommand(key, command, arg, data);
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
    public Object doCommandAsObject(Data key, Command command, String arg, Data... data) {
        return doCommandAsObject(key, command, new String[]{arg}, data);
    }

    public boolean doCommandAsBoolean(Data key, Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(key, command, args, datas);
        return Boolean.valueOf(protocol.args[0]);
    }

    public int doCommandAsInt(Data key, Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(key, command, args, datas);
        return Integer.valueOf(protocol.args[0]);
    }

    public <E> List<E> doCommandAsList(Data key, Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(key, command, args, datas);
        List<E> list = new ArrayList<E>();
        if (protocol.hasBuffer()) {
            for (Data bb : protocol.buffers) {
                list.add((E) ss.toObject(bb));
            }
        }
        return list;
    }

    protected Protocol lock(String name, Data key, Command command, String[] args, Data data) {
        Context context = ensureContextHasConnection(key);
        context.incrementAndGet(name, key.hashCode());
        return doCommand(key, command, args, data);
    }

    protected Context ensureContextHasConnection(Data key) {
        Context context = Context.getOrCreate();
        if (context.getConnection() == null) {
            try {
                Connection connection = cp.takeConnection(key);
                context.setConnection(connection);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return context;
    }

    protected Protocol unlock(String name, Data key, Command command, String[] args, Data data) {
        Context context = Context.get();
        if (context == null) {
            throw new IllegalStateException("You don't seem to have a lock to unlock.");
        }
        Protocol response = doCommand(key, command, args, data);
        if (context.decrementAndGet(name, key.hashCode()) == 0 && context.noMoreLocks()) {
            Connection connection = context.getConnection();
            cp.releaseConnection(connection, key);
            Context.remove();
        }
        return response;
    }
}
