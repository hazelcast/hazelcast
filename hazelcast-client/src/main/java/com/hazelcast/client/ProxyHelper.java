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

import com.hazelcast.client.impl.CollectionWrapper;
import com.hazelcast.core.Member;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.Collection;
import java.util.EventListener;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    final AtomicLong count = new AtomicLong();
    final AtomicLong sent = new AtomicLong();
    final AtomicLong written = new AtomicLong();
    final AtomicLong received = new AtomicLong();
    final AtomicLong replied = new AtomicLong();

    protected Packet callAndGetResult(Packet request) {
        long theCount = count.incrementAndGet();
        long now = System.nanoTime();
        Call c = createCall(request);
        Packet response = doCall(c);
        sent.addAndGet(c.sent - now);
        written.addAndGet(c.written - c.sent);
        received.addAndGet(c.received - c.written);
        replied.addAndGet(c.replied - c.received);
//        if (theCount == 30000) {
//            count.addAndGet(-30000);
//            System.out.println("sent " + (sent.getAndSet(0) / theCount / 1000));
//            System.out.println("written " + (written.getAndSet(0) / theCount / 1000));
//            System.out.println("received " + (received.getAndSet(0) / theCount / 1000));
//            System.out.println("replied " + (replied.getAndSet(0) / theCount / 1000));
//        }
        return response;
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
        return createRequestPacket(operation, key, value, 0, null);
    }

    private Packet createRequestPacket(ClusterOperation operation, byte[] key, byte[] value, long ttl, TimeUnit timeunit) {
        Packet request = createRequestPacket();
        request.setOperation(operation);
        request.setKey(key);
        request.setValue(value);
        if (ttl > 0 && timeunit != null) {
            request.setTimeout(timeunit.toMillis(ttl));
        }
        return request;
    }

    <V> Future<V> doAsync(final ClusterOperation operation, final Object key, final Object value) {
        Packet request = prepareRequest(operation, key, value);
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

    private Packet prepareRequest(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit timeunit) {
        byte[] k = null;
        byte[] v = null;
        if (key != null) {
            k = toByte(key);
        }
        if (value != null) {
            v = toByte(value);
        }
        return createRequestPacket(operation, k, v, ttl, timeunit);
    }

    protected Packet prepareRequest(ClusterOperation operation, Object key,
                                    Object value) {
        return prepareRequest(operation, key, value, 0, null);
    }

    protected Object getValue(Packet response) {
        if (response.getValue() != null) {
            return toObject(response.getValue());
        }
        return null;
    }

    public void destroy() {
        doOp(ClusterOperation.DESTROY, null, null);
        this.client.destroy(name);
    }

    public <K> Collection<K> keys(Predicate predicate) {
        return ((CollectionWrapper<K>) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, predicate)).getKeys();
    }

    public <K> Collection<K> entries(final Predicate predicate) {
        return ((CollectionWrapper<K>) doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES, null, predicate)).getKeys();
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
