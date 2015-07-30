/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.collection.impl.queue.QueueIterator;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * @author ali 5/19/13
 */
public final class ClientQueueProxy<E> extends ClientProxy implements IQueue<E> {

    private final String name;

    public ClientQueueProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    @Override
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        ClientMessage request = QueueAddListenerCodec.encodeRequest(name, includeValue);

        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(includeValue, listener);
        return listen(request, getPartitionKey(), eventHandler);
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
        public void handle(Data dataItem, String uuid, int eventType) {
            SerializationService serializationService = getContext().getSerializationService();
            ClientClusterService clusterService = getContext().getClusterService();

            E item = includeValue ? (E) serializationService.toObject(dataItem) : null;
            Member member = clusterService.getMember(uuid);
            ItemEvent<E> itemEvent = new ItemEvent<E>(name, ItemEventType.getByType(eventType), item, member);
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


    public boolean removeItemListener(String registrationId) {
        return stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return QueueRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return QueueRemoveListenerCodec.decodeResponse(clientMessage).response;
            }
        });
    }

    public LocalQueueStats getLocalQueueStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

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
    public boolean offer(E e) {
        try {
            return offer(e, 0, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            return false;
        }
    }

    public void put(E e) throws InterruptedException {
        Data data = toData(e);
        ClientMessage request = QueuePutCodec.encodeRequest(name, data);
        invokeInterruptibly(request);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        Data data = toData(e);
        ClientMessage request = QueueOfferCodec.encodeRequest(name, data, unit.toMillis(timeout));
        ClientMessage response = invokeInterruptibly(request);
        QueueOfferCodec.ResponseParameters resultParameters = QueueOfferCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public E take() throws InterruptedException {
        ClientMessage request = QueueTakeCodec.encodeRequest(name);
        ClientMessage response = invokeInterruptibly(request);
        QueueTakeCodec.ResponseParameters resultParameters = QueueTakeCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = QueuePollCodec.encodeRequest(name, unit.toMillis(timeout));
        ClientMessage response = invokeInterruptibly(request);
        QueuePollCodec.ResponseParameters resultParameters = QueuePollCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public int remainingCapacity() {
        ClientMessage request = QueueRemainingCapacityCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueRemainingCapacityCodec.ResponseParameters resultParameters = QueueRemainingCapacityCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean remove(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueRemoveCodec.encodeRequest(name, data);
        ClientMessage response = invoke(request);
        QueueRemoveCodec.ResponseParameters resultParameters = QueueRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean contains(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueContainsCodec.encodeRequest(name, data);
        ClientMessage response = invoke(request);
        QueueContainsCodec.ResponseParameters resultParameters = QueueContainsCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public int drainTo(Collection<? super E> objects) {
        ClientMessage request = QueueDrainToCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueDrainToCodec.ResponseParameters resultParameters = QueueDrainToCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        for (Data data : resultCollection) {
            E e = toObject(data);
            objects.add(e);
        }
        return resultCollection.size();
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        ClientMessage request = QueueDrainToMaxSizeCodec.encodeRequest(name, maxElements);
        ClientMessage response = invoke(request);
        QueueDrainToMaxSizeCodec.ResponseParameters resultParameters = QueueDrainToMaxSizeCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        for (Data data : resultCollection) {
            E e = toObject(data);
            c.add(e);
        }
        return resultCollection.size();
    }

    public E remove() {
        final E res = poll();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public E poll() {
        try {
            return poll(0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public E element() {
        final E res = peek();
        if (res == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return res;
    }

    public E peek() {
        ClientMessage request = QueuePeekCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueuePeekCodec.ResponseParameters resultParameters = QueuePeekCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public int size() {
        ClientMessage request = QueueSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueSizeCodec.ResponseParameters resultParameters = QueueSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean isEmpty() {
        ClientMessage request = QueueIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueIsEmptyCodec.ResponseParameters resultParameters = QueueIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public Iterator<E> iterator() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        return new QueueIterator<E>(resultCollection.iterator(), getContext().getSerializationService(), false);
    }

    public Object[] toArray() {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        int i = 0;
        Object[] array = new Object[resultCollection.size()];
        for (Data data : resultCollection) {
            array[i++] = toObject(data);
        }
        return array;
    }

    public <T> T[] toArray(T[] ts) {
        ClientMessage request = QueueIteratorCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        QueueIteratorCodec.ResponseParameters resultParameters = QueueIteratorCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
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

    public boolean containsAll(Collection<?> c) {
        ClientMessage request = QueueContainsAllCodec.encodeRequest(name, getDataList(c));
        ClientMessage response = invoke(request);
        QueueContainsAllCodec.ResponseParameters resultParameters = QueueContainsAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean addAll(Collection<? extends E> c) {
        ClientMessage request = QueueAddAllCodec.encodeRequest(name, getDataList(c));
        ClientMessage response = invoke(request);
        QueueAddAllCodec.ResponseParameters resultParameters = QueueAddAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean removeAll(Collection<?> c) {
        ClientMessage request = QueueCompareAndRemoveAllCodec.encodeRequest(name, getDataList(c));
        ClientMessage response = invoke(request);
        QueueCompareAndRemoveAllCodec.ResponseParameters resultParameters = QueueCompareAndRemoveAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean retainAll(Collection<?> c) {
        ClientMessage request = QueueCompareAndRetainAllCodec.encodeRequest(name, getDataList(c));
        ClientMessage response = invoke(request);
        QueueCompareAndRetainAllCodec.ResponseParameters resultParameters = QueueCompareAndRetainAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public void clear() {
        ClientMessage request = QueueClearCodec.encodeRequest(name);
        invoke(request);
    }

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getPartitionKey());
    }

    protected <T> T invokeInterruptibly(ClientMessage req) throws InterruptedException {
        return super.invokeInterruptibly(req, getPartitionKey());
    }

    private List<Data> getDataList(Collection<?> objects) {
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(toData(o));
        }
        return dataList;
    }

    @Override
    public String toString() {
        return "IQueue{" + "name='" + getName() + '\'' + '}';
    }
}
