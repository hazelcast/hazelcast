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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class ProxyHelper {

    private final static AtomicLong callIdGen = new AtomicLong(0);
    protected final HazelcastClient client;

    final ILogger logger = com.hazelcast.logging.Logger.getLogger(this.getClass().getName());

    public ProxyHelper(HazelcastClient client) {
        this.client = client;
    }

    public int getCurrentThreadId() {
        return (int) Thread.currentThread().getId();
    }

    public static Long newCallId() {
        return callIdGen.incrementAndGet();
    }

    public void sendCall(final Call c) {
        if (c == null) {
            throw new NullPointerException();
        }
        client.getOutRunnable().enQueue(c);
        c.sent = System.nanoTime();
    }

    protected Object doCall(Call c) {
        sendCall(c);
        return getResponse(c);
    }
//    Packet prepareRequest(ClusterOperation operation, Object key, Object value, long ttl, TimeUnit timeunit) {
//        final ThreadContext threadContext = ThreadContext.get();
//        threadContext.setCurrentSerializerRegistry(client.getSerializationService());
//        byte[] k = null;
//        byte[] v = null;
//        if (key != null) {
//            k = threadContext.toByteArray(key);
//        }
//        if (value != null) {
//            v = threadContext.toByteArray(value);
//        }
//        Packet packet = createRequestPacket(operation, k, v, ttl, timeunit);
//        if (key instanceof PartitionAware) {
//            Object partitionKey = ((PartitionAware) key).getPartitionKey();
//            if (partitionKey == null) throw new IllegalArgumentException("PartitionKey cannot be null!");
//            packet.setKeyHash(Util.hashCode(threadContext.toByteArray(partitionKey)));
//        }
//        if (value instanceof PartitionAware) {
//            Object partitionKey = ((PartitionAware) value).getPartitionKey();
//            if (partitionKey == null) throw new IllegalArgumentException("PartitionKey cannot be null!");
//            packet.setValueHash(Util.hashCode(threadContext.toByteArray(partitionKey)));
//        }
//        return packet;
//    }
//    Packet prepareRequest(ClusterOperation operation, Object key,
//                          Object value) {
//        return prepareRequest(operation, key, value, 0, null);
//
//    }

    protected Object getResponse(Call c, long timeout, TimeUnit timeUnit) {
        final Object response = c.getResponse(timeout, timeUnit);
        if (response != null) {
            c.replied = System.nanoTime();
            c.end();
        }
        if (!client.isActive()) {
            throw new RuntimeException("HazelcastClient is no longer active.");
        }
        return response;
    }

    protected Object getResponse(Call c) {
        final int timeout = 5;
        for (int i = 0; ; i++) {
            final Object response = getResponse(c, timeout, TimeUnit.SECONDS);
            if (i > 0) {
                logger.log(Level.INFO, "There is no response for " + c
                        + " in " + (timeout * i) + " seconds.");
            }
            if (response != null) {
                return response;
            }
        }
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

    static void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
//        if (!(obj instanceof Serializable)) {
//            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
//        }
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

    public Data toData(Object obj) {
        return client.getSerializationService().toData(obj);
    }

    public Object toObject(Data data) {
        return client.getSerializationService().toObject(data);
    }
}
