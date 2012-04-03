/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.core.PartitionAware;
import com.hazelcast.impl.ClientServiceException;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.Keys;
import com.hazelcast.impl.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.client.IOUtil.toByte;
import static com.hazelcast.client.IOUtil.toObject;

public class ProxyHelper {
    private final static AtomicLong callIdGen = new AtomicLong(0);
    private final String name;
    private final HazelcastClient client;

    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());

    public ProxyHelper(String name, HazelcastClient client) {
        this.name = (name == null) ? "" : name;
        this.client = client;
    }

    public String getName() {
        return name;
    }

    protected Packet callAndGetResult(Packet request) {
        Call c = createCall(request);
        return doCall(c);
    }

    protected Packet doCall(Call c) {
        sendCall(c);
        c.sent = System.nanoTime();
        final int timeout = 5;
        for (int i = 0; ; i++) {
            final Object response = c.getResponse(timeout, TimeUnit.SECONDS);
            if (response != null) {
                c.replied = System.nanoTime();
                return (Packet) response;
            }
            if (i > 0) {
                logger.log(Level.INFO, "There is no response for " + c
                        + " in " + (timeout * i) + " seconds.");
            }
            if (!client.isActive()) {
                throw new RuntimeException("HazelcastClient is no longer active.");
            }
        }
    }

    public void sendCall(final Call c) {
        if (c == null) {
            throw new NullPointerException();
        }
        client.getOutRunnable().enQueue(c);
    }

    public Call createCall(Packet request) {
        final Long id = newCallId();
        return new Call(id, request) {
            @Override
            public void onDisconnect(Member member) {
                if (!client.getOutRunnable().queue.contains(this)) {
                    logger.log(Level.FINEST, "Re enqueue " + this);
                    client.getOutRunnable().enQueue(this);
                }
            }
        };
    }

    public static Long newCallId() {
        return callIdGen.incrementAndGet();
    }

    private Packet createRequestPacket() {
        Packet request = new Packet();
        request.setName(name);
        request.setThreadId(getCurrentThreadId());
        return request;
    }

    public int getCurrentThreadId() {
        return (int) Thread.currentThread().getId();
    }

    public Packet createRequestPacket(ClusterOperation operation, byte[] key, byte[] value) {
        return createRequestPacket(operation, key, value, 0, null, 0, null);
    }

    private Packet createRequestPacket(ClusterOperation operation, byte[] key, byte[] value, long ttl, TimeUnit ttlTimeunit, long timeout, TimeUnit timeunit) {
        Packet request = createRequestPacket();
        request.setOperation(operation);
        request.setKey(key);
        request.setValue(value);
        //FIXME: sending timeout as ttl and ttl as timeout for backward compatibility
        if (ttl >= 0 && ttlTimeunit != null) {
            request.setTimeout(ttlTimeunit.toMillis(ttl));
        }
        if (timeout >= 0 && timeunit != null) {
            request.setTtl(timeunit.toMillis(timeout));
        }
        return request;
    }

    <V> Future<V> doAsync(final ClusterOperation operation, final Object key, final Object value) {
        Packet request = prepareRequest(operation, key, value);
        Call remoteCall = createCall(request);
        sendCall(remoteCall);
        return new AsyncClientCall<V>(remoteCall);
    }

    <V> Future<V> doAsync(final ClusterOperation operation, final Object key, final Object value, long ttl, TimeUnit timeunit) {
        Packet request = prepareRequest(operation, key, value, ttl, timeunit);
        Call remoteCall = createCall(request);
        sendCall(remoteCall);
        return new AsyncClientCall<V>(remoteCall);
    }

    protected Object doOp(ClusterOperation operation, Object key, Object value) {
        return doOp(operation, key, value, 0, null);
    }

    public Object doOp(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit timeunit) {
        Packet request = prepareRequest(operation, key, value, ttl, timeunit);
        Packet response = callAndGetResult(request);
        return getValue(response);
    }

    public Object doOpTtlTimeout(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit ttlTimeunit, long timeout, TimeUnit timeunit) {
        Packet request = prepareRequest(operation, key, value, ttl, ttlTimeunit, timeout, timeunit);
        Packet response = callAndGetResult(request);
        return getValue(response);
    }

    public void doFireAndForget(ClusterOperation operation, Object key, Object value) {
        Packet request = prepareRequest(operation, key, value, 0, null);
        Call fireNForgetCall = createCall(request);
        fireNForgetCall.setFireNforget(true);
        sendCall(fireNForgetCall);
    }

    Packet prepareRequest(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit ttlTimeunit, long timeout, TimeUnit timeunit) {
        byte[] k = null;
        byte[] v = null;
        if (key != null) {
            k = toByte(key);
        }
        if (value != null) {
            v = toByte(value);
        }
        Packet packet = createRequestPacket(operation, k, v, ttl, ttlTimeunit, timeout, timeunit);
        if (key instanceof PartitionAware) {
            Object partitionKey = ((PartitionAware) key).getPartitionKey();
            if (partitionKey == null) throw new IllegalArgumentException("PartitionKey cannot be null!");
            packet.setKeyHash(Util.hashCode(toByte(partitionKey)));
        }
        if (value instanceof PartitionAware) {
            Object partitionKey = ((PartitionAware) value).getPartitionKey();
            if (partitionKey == null) throw new IllegalArgumentException("PartitionKey cannot be null!");
            packet.setValueHash(Util.hashCode(toByte(partitionKey)));
        }
        return packet;
    }

    Packet prepareRequest(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit timeunit) {
        return prepareRequest(operation, key, value, ttl, timeunit, -1, null);
    }

    Packet prepareRequest(ClusterOperation operation, Object key,
                          Object value) {
        return prepareRequest(operation, key, value, 0, null);
    }

    static Object getValue(Packet response) {
        if (response.getValue() != null) {
            Object result = toObject(response.getValue());
            if (result instanceof ClientServiceException) {
                final ClientServiceException ex = (ClientServiceException) result;
                Util.throwUncheckedException(ex.getThrowable());
            }
            return result;
        }
        return null;
    }

    public void destroy() {
        doOp(ClusterOperation.DESTROY, null, null);
        this.client.destroy(name);
    }

    public <K> Collection<K> keys(Predicate predicate) {
        Keys keys = (Keys) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, predicate);
        Collection<K> collection = new ArrayList<K>();
        for (Data d : keys) {
            collection.add((K) toObject(d.buffer));
        }
        return collection;
    }

    public <K> Collection<K> entries(final Predicate predicate) {
        Keys keys = (Keys) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, null, predicate);
        Collection<K> collection = new ArrayList<K>();
        for (Data d : keys) {
            collection.add((K) toObject(d.buffer));
        }
        return collection;
    }

    static void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    static void check(EventListener listener) {
        if (listener == null) {
            throw new NullPointerException("Listener can not be null");
        }
    }

    static void checkTime(long time, TimeUnit timeunit) {
        if (time < 0) {
            throw new IllegalArgumentException("Time can not be less than 0.");
        }
        if (timeunit == null) {
            throw new NullPointerException("TimeUnit can not be null.");
        }
    }

    public HazelcastClient getHazelcastClient() {
        return client;
    }
}
