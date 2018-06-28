/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
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
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.queue.QueueIterator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;
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

    @Override
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        isNotNull(listener, "listener");
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
            public String decodeAddResponse(ClientMessage clientMessage) {
                return QueueAddListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return QueueRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return QueueRemoveListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    private class ItemEventHandler extends ListAddListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final boolean includeValue;
        private final ItemListener<E> listener;

        public ItemEventHandler(boolean includeValue, ItemListener<E> listener) {
            this.includeValue = includeValue;
            this.listener = listener;
        }

        @Override
        public void handleItemEventV10(Data dataItem, String uuid, int eventType) {
            Member member = getContext().getClusterService().getMember(uuid);
            ItemEvent<E> itemEvent = new DataAwareItemEvent(name, ItemEventType.getByType(eventType),
                    dataItem, member, getSerializationService());
            if (eventType == ItemEventType.ADDED.getType()) {
                listener.itemAdded(itemEvent);
            } else {
                listener.itemRemoved(itemEvent);
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    @Override
    public boolean removeItemListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public boolean add(E e) {
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
    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        Data data = toData(e);
        ClientMessage request = QueuePutCodec.encodeRequest(name, data);
        invokeOnPartitionInterruptibly(request);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(e, "item can't be null");

        Data data = toData(e);
        ClientMessage request = QueueOfferCodec.encodeRequest(name, data, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        QueueOfferCodec.ResponseParameters resultParameters = QueueOfferCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public E take() throws InterruptedException {
        ClientMessage request = QueueTakeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        QueueTakeCodec.ResponseParameters resultParameters = QueueTakeCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = QueuePollCodec.encodeRequest(name, unit.toMillis(timeout));
        ClientMessage response = invokeOnPartitionInterruptibly(request);
        QueuePollCodec.ResponseParameters resultParameters = QueuePollCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public int remainingCapacity() {
        ClientMessage request = QueueRemainingCapacityCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueRemainingCapacityCodec.ResponseParameters resultParameters = QueueRemainingCapacityCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean remove(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueRemoveCodec.encodeRequest(name, data);
        ClientMessage response = invokeOnPartition(request);
        QueueRemoveCodec.ResponseParameters resultParameters = QueueRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean contains(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueContainsCodec.encodeRequest(name, data);
        ClientMessage response = invokeOnPartition(request);
        QueueContainsCodec.ResponseParameters resultParameters = QueueContainsCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public int drainTo(Collection<? super E> objects) {
        ClientMessage request = QueueDrainToCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueDrainToCodec.ResponseParameters resultParameters = QueueDrainToCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.response;
        for (Data data : resultCollection) {
            E e = toObject(data);
            objects.add(e);
        }
        return resultCollection.size();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        ClientMessage request = QueueDrainToMaxSizeCodec.encodeRequest(name, maxElements);
        ClientMessage response = invokeOnPartition(request);
        QueueDrainToMaxSizeCodec.ResponseParameters resultParameters = QueueDrainToMaxSizeCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.response;
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
        QueuePeekCodec.ResponseParameters resultParameters = QueuePeekCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    @Override
    public int size() {
        ClientMessage request = QueueSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueSizeCodec.ResponseParameters resultParameters = QueueSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = QueueIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueIsEmptyCodec.ResponseParameters resultParameters = QueueIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public Iterator<E> iterator() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.response;
        return new QueueIterator<E>(resultCollection.iterator(), getSerializationService(), false);
    }

    @Override
    public Object[] toArray() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.response;
        int i = 0;
        Object[] array = new Object[resultCollection.size()];
        for (Data data : resultCollection) {
            array[i++] = toObject(data);
        }
        return array;
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.response;
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
    public boolean containsAll(Collection<?> c) {
        checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueContainsAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        QueueContainsAllCodec.ResponseParameters resultParameters = QueueContainsAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueAddAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        QueueAddAllCodec.ResponseParameters resultParameters = QueueAddAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueCompareAndRemoveAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        QueueCompareAndRemoveAllCodec.ResponseParameters resultParameters =
                QueueCompareAndRemoveAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = QueueCompareAndRetainAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        QueueCompareAndRetainAllCodec.ResponseParameters resultParameters =
                QueueCompareAndRetainAllCodec.decodeResponse(response);
        return resultParameters.response;
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
