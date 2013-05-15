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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.connection.ConnectionManager;
import com.hazelcast.client.connection.ProtocolReader;
import com.hazelcast.client.connection.ProtocolWriter;
import com.hazelcast.client.exception.ClusterClientException;
import com.hazelcast.client.proxy.listener.ListenerResponseHandler;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.deprecated.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.core.Partition;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EventListener;
import java.util.List;
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
    final ConnectionManager cp;
    final PartitionClientProxy pp;
    final boolean smart;
    final boolean redo;
    private final SerializationService ss;

    public ProxyHelper(HazelcastClient client) {
        this.cp = client.getConnectionPool();
        this.ss = client.getSerializationService();
        this.pp = (PartitionClientProxy) client.getPartitionService();
        this.writer = new ProtocolWriter(ss);
        this.reader = new ProtocolReader(ss);
        smart = client.getClientConfig().isSmart();
        redo = client.getClientConfig().isRedoOperation();
    }

    public int getCurrentThreadId() {
        return (int) Thread.currentThread().getId();
    }

    public static Long newCallId() {
        return callIdGen.incrementAndGet();
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

    public Object getSingleObjectFromResponse(Protocol response) {
        if (response != null && response.buffers != null && response.hasBuffer()) {
            return ss.toObject(response.buffers[0]);
        } else return null;
    }

    public Protocol createProtocol(Command command, String[] args, Data[] data) {
        if (args == null) args = new String[]{};
        long id = newCallId();
        Protocol protocol = new Protocol(null, command, String.valueOf(id), getCurrentThreadId(), false, args, data);
        return protocol;
    }

    public void doFireNForget(Command command, String[] args, Data... data) {
        Protocol protocol = createProtocol(command, args, data);
        try {
            protocol.onEnqueue();
            Connection connection = cp.takeConnection(null);
            writer.write(connection, protocol);
            cp.releaseConnection(connection);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
        }
    }

    public ListenerThread createAListenerThread(String threadName, HazelcastClient client, Protocol request, ListenerResponseHandler lrh) {
        ConnectionManager cp = client.getConnectionPool();
        LoadBalancer rt = cp.getRouter();
        Member member = rt.next();
        InetSocketAddress isa;
        if (member == null)
            isa = client.getCluster().getMembers().iterator().next().getInetSocketAddress();
        else
            isa = member.getInetSocketAddress();
        try {
            Connection connection = cp.newConnection(new Address(isa));
            return new ListenerThread(threadName, request, lrh, connection, ss);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Member key2MemberIfSmart(Object object) {
        if (!smart) return null;
        Partition partition = pp.getCachedPartition(object);
        Member owner = partition == null ? null : partition.getOwner();
        return owner;
    }

    public Protocol doCommand(Data key, Command command, String[] args, Data... data) {
        Member member = key == null ? null : key2MemberIfSmart(key);
        return doCommand(member, command, args, data);
    }

    public Protocol doCommand(Command command, String[] args, Data... data) {
        return doCommand((Member) null, command, args, data);
    }

    /**
     * Expects one binary data on response and returns the deserialized version of that binary data.
     *
     * @param command
     * @param args
     * @param data
     * @return
     */
    public Object doCommandAsObject(Data key, Command command, String[] args, Data... data) {
        Protocol response = doCommand(key, command, args, data);
        return getSingleObjectFromResponse(response);
    }

    /**
     * Expects one binary data on response and returns the deserialized version of that binary data.
     *
     * @param command
     * @param args
     * @param data
     * @return
     */
    public Object doCommandAsObject(Command command, String[] args, Data... data) {
        return doCommandAsObject(null, command, args, data);
    }

    public boolean doCommandAsBoolean(Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(command, args, datas);
        return Boolean.valueOf(protocol.args[0]);
    }

    public boolean doCommandAsBoolean(Data key, Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(key, command, args, datas);
        return Boolean.valueOf(protocol.args[0]);
    }

    public int doCommandAsInt(Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(command, args, datas);
        return Integer.valueOf(protocol.args[0]);
    }

    public int doCommandAsInt(Data key, Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(key, command, args, datas);
        return Integer.valueOf(protocol.args[0]);
    }

    public long doCommandAsLong(Command command, String[] args, Data... datas) {
        Protocol protocol = doCommand(command, args, datas);
        return Long.valueOf(protocol.args[0]);
    }

    public <E> List<E> doCommandAsList(Command command, String[] args, Data... datas) {
        return doCommandAsList(null, command, args, datas);
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

    public Protocol doCommand(Member member, Command command, String[] args, Data... data) {
        try {
            Protocol protocol = createProtocol(command, args, data);
            protocol.onEnqueue();
            Context context = Context.get();
            Connection connection = context == null ? null : context.getConnection();
            if (connection == null) {
                connection = cp.takeConnection(member);
            }
            writer.write(connection, protocol);
            writer.flush(connection);
            Protocol response = reader.read(connection);
            cp.releaseConnection(connection);
            if (Command.OK.equals(response.command))
                return response;
            else if (response.hasBuffer() || response.args.length > 0) {
                if (response.hasBuffer()) {
                    Exception e = (Exception) toObject(response.buffers[0]);
                    if (e instanceof RuntimeException) throw (RuntimeException) e;
                    else throw new ClusterClientException(e);
                } else {
                    throw new ClusterClientException(response.args[0]);
                }
            } else {
                throw new RuntimeException(response.command + ": " + Arrays.asList(response.args));
            }
        } catch (HazelcastInstanceNotActiveException e) {
            sleep(1000);
            return doCommand((Member) null, command, args, data);
        } catch (IOException e) {
            if (redo) {
                sleep(1000);
                return doCommand((Member) null, command, args, data);
            } else throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
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

    protected Protocol lock(String name, Data key, Command command, String[] args, Data data) {
        Context context = ensureContextHasConnection(key);
        context.incrementAndGet(name, key.hashCode());
        return doCommand(key, command, args, data);
    }

    protected Context ensureContextHasConnection(Data key) {
        Context context = Context.getOrCreate();
        if (context.getConnection() == null) {
            try {
                Member member = key2MemberIfSmart(key);
                Connection connection = cp.takeConnection(member);
                context.setConnection(connection);
            } catch (InterruptedException e) {
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
            cp.releaseConnection(connection);
            Context.remove();
        }
        return response;
    }
}
