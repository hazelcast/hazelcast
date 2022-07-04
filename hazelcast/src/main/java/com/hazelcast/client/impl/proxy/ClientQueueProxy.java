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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.QueueAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.QueueClearCodec;
import com.hazelcast.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.QueueContainsCodec;
import com.hazelcast.client.impl.protocol.codec.QueueDrainToCodec;
import com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec;
import com.hazelcast.client.impl.protocol.codec.QueueIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.QueueIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.QueueOfferCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePeekCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePollCodec;
import com.hazelcast.client.impl.protocol.codec.QueuePutCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemainingCapacityCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.QueueRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.QueueSizeCodec;
import com.hazelcast.client.impl.protocol.codec.QueueTakeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.queue.QueueIterator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link IQueue}.
 *
 * @param <E> the type of elements in this queue
 */
public final class ClientQueueProxy<E> extends PartitionSpecificClientProxy implements IQueue<E> {

    public ClientQueueProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Nonnull
    @Override
    public UUID addItemListener(@Nonnull ItemListener<E> listener, boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(includeValue, listener);
        return registerListener(createItemListenerCodec(includeValue), eventHandler);
    }

    private ListenerMessageCodec createItemListenerCodec(final boolean includeValue) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return QueueAddListenerCodec.encodeRequest(name, includeValue, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return QueueAddListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return QueueRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return QueueRemoveListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    private class ItemEventHandler extends QueueAddListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final boolean includeValue;
        private final ItemListener<E> listener;

        ItemEventHandler(boolean includeValue, ItemListener<E> listener) {
            this.includeValue = includeValue;
            this.listener = listener;
        }

        @Override
        public void handleItemEvent(Data dataItem, UUID uuid, int eventType) {
            Member member = getContext().getClusterService().getMember(uuid);
            ItemEvent<E> itemEvent = new DataAwareItemEvent(name, ItemEventType.getByType(eventType),
                    dataItem, member, getSerializationService());
            if (eventType == ItemEventType.ADDED.getType()) {
                listener.itemAdded(itemEvent);
            } else {
                listener.itemRemoved(itemEvent);
            }
        }
    }

    @Override
    public boolean removeItemListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Null registrationId is not allowed!");
        return deregisterListener(registrationId);
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public boolean add(@Nonnull E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    /**
     * It is advised to use this method in a try-catch block to take the offer operation
     * full lifecycle control, in a "lost node" scenario you can not be sure
     * offer is succeeded or not so you may want to retry.
     *
     * @param e the element to add
     * @return <tt>true</tt> if the element was added to this queue.
     * <tt>false</tt> if there is not enough capacity to insert the element.
     * @throws HazelcastException if client loses the connected node.
     */
    @Override
    public boolean offer(@Nonnull E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void put(@Nonnull E e) throws InterruptedException {
        checkNotNull(e, "Null item is not allowed!");

        Data data = toData(e);
        ClientMessage request = QueuePutCodec.encodeRequest(name, data);
        invokeOnPartitionInterruptibly(request);
    }

    @Override
    public boolean offer(@Nonnull E e,
                         long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        checkNotNull(e, "Null item is not allowed!");
        checkNotNull(unit, "Null timeUnit is not allowed!");

        Data data = toData(e);
        ClientMessage request = QueueOfferCodec.encodeRequest(name, data, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        return QueueOfferCodec.decodeResponse(response);
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        ClientMessage request = QueueTakeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        return toObject(QueueTakeCodec.decodeResponse(response));
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        checkNotNull(unit, "Null timeUnit is not allowed!");

        ClientMessage request = QueuePollCodec.encodeRequest(name, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        return toObject(QueuePollCodec.decodeResponse(response));
    }

    @Override
    public int remainingCapacity() {
        ClientMessage request = QueueRemainingCapacityCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return QueueRemainingCapacityCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed!");
        Data data = toData(o);
        ClientMessage request = QueueRemoveCodec.encodeRequest(name, data);
        ClientMessage response = invokeOnPartition(request);
        return QueueRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean contains(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed!");
        Data data = toData(o);
        ClientMessage request = QueueContainsCodec.encodeRequest(name, data);
        ClientMessage response = invokeOnPartition(request);
        return QueueContainsCodec.decodeResponse(response);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> objects) {
        checkNotNull(objects, "Null objects parameter is not allowed!");

        ClientMessage request = QueueDrainToCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        Collection<Data> resultCollection = QueueDrainToCodec.decodeResponse(response);
        for (Data data : resultCollection) {
            E e = toObject(data);
            objects.add(e);
        }
        return resultCollection.size();
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> c, int maxElements) {
        checkNotNull(c, "Null collection parameter is not allowed!");

        ClientMessage request = QueueDrainToMaxSizeCodec.encodeRequest(name, maxElements);
        ClientMessage response = invokeOnPartition(request);
        Collection<Data> resultCollection = QueueDrainToMaxSizeCodec.decodeResponse(response);
        for (Data data : resultCollection) {
            E e = toObject(data);
            c.add(e);
        }
        return resultCollection.size();
    }

    @Override
    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    @Override
    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
    }

    @Override
    public E element() {
        final E res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    @Override
    public E peek() {
        ClientMessage request = QueuePeekCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return toObject(QueuePeekCodec.decodeResponse(response));
    }

    @Override
    public int size() {
        ClientMessage request = QueueSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return QueueSizeCodec.decodeResponse(response);
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = QueueIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return QueueIsEmptyCodec.decodeResponse(response);
    }

    @Override
    public Iterator<E> iterator() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        Collection<Data> resultCollection = QueueIteratorCodec.decodeResponse(response);
        return new QueueIterator<E>(resultCollection.iterator(), getSerializationService(), false);
    }

    @Override
    public Object[] toArray() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        Collection<Data> resultCollection = QueueIteratorCodec.decodeResponse(response);
        int i = 0;
        Object[] array = new Object[resultCollection.size()];
        for (Data data : resultCollection) {
            array[i++] = toObject(data);
        }
        return array;
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        checkNotNull(ts, "Null array parameter is not allowed!");

        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        Collection<Data> resultCollection = QueueIteratorCodec.decodeResponse(response);
        int size = resultCollection.size();
        if (ts.length < size) {
            ts = (T[]) java.lang.reflect.Array.newInstance(ts.getClass().getComponentType(), size);
        }
        int i = 0;
        for (Data data : resultCollection) {
            ts[i++] = (T) toObject(data);
        }
        return ts;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed!");

        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueContainsAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return QueueContainsAllCodec.decodeResponse(response);
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed!");

        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueAddAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return QueueAddAllCodec.decodeResponse(response);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed!");

        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueCompareAndRemoveAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return QueueCompareAndRemoveAllCodec.decodeResponse(response);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed!");

        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueCompareAndRetainAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return QueueCompareAndRetainAllCodec.decodeResponse(response);
    }

    @Override
    public void clear() {
        ClientMessage request = QueueClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Override
    public String toString() {
        return "IQueue{" + "name='" + name + '\'' + '}';
    }
}
