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
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueAddAllParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueClearParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueCompareAndRemoveAllParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueCompareAndRetainAllParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueContainsAllParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueContainsParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueDrainToMaxSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueIsEmptyParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueIteratorParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueOfferParameters;
import com.hazelcast.client.impl.protocol.parameters.QueuePeekParameters;
import com.hazelcast.client.impl.protocol.parameters.QueuePollParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueRemainingCapacityParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueRemoveListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.QueueSizeParameters;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.collection.impl.queue.QueueIterator;
import com.hazelcast.collection.impl.queue.client.AddListenerRequest;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableItemEvent;

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

    //Todo needs QueueAddListenerMessageTask call()
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        final AddListenerRequest request = new AddListenerRequest(name, includeValue);
        EventHandler<PortableItemEvent> eventHandler = new EventHandler<PortableItemEvent>() {
            public void handle(PortableItemEvent portableItemEvent) {
                E item = includeValue ? (E) getContext().getSerializationService().toObject(portableItemEvent.getItem()) : null;
                Member member = getContext().getClusterService().getMember(portableItemEvent.getUuid());
                ItemEvent<E> itemEvent = new ItemEvent<E>(name, portableItemEvent.getEventType(), item, member);
                if (portableItemEvent.getEventType() == ItemEventType.ADDED) {
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
        };
        return listen(request, getPartitionKey(), eventHandler);
    }

    public boolean removeItemListener(String registrationId) {
        ClientMessage request = QueueRemoveListenerParameters.encode(name, registrationId);
        return stopListening(request, registrationId);
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
        offer(e, -1, TimeUnit.MILLISECONDS);
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        Data data = toData(e);
        ClientMessage request = QueueOfferParameters.encode(name, data, unit.toMillis(timeout));
        ClientMessage response = invokeInterruptibly(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        ClientMessage request = QueuePollParameters.encode(name);
        ClientMessage response = invokeInterruptibly(request);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    public int remainingCapacity() {
        ClientMessage request = QueueRemainingCapacityParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean remove(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueRemoveParameters.encode(name, data);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean contains(Object o) {
        Data data = toData(o);
        ClientMessage request = QueueContainsParameters.encode(name, data);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        ClientMessage request = QueueDrainToMaxSizeParameters.encode(name, maxElements);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
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
        ClientMessage request = QueuePeekParameters.encode(name);
        ClientMessage response = invoke(request);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    public int size() {
        ClientMessage request = QueueSizeParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean isEmpty() {
        ClientMessage request = QueueIsEmptyParameters.encode(name);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public Iterator<E> iterator() {
        ClientMessage request = QueueIteratorParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
        return new QueueIterator<E>(resultCollection.iterator(), getContext().getSerializationService(), false);
    }

    public Object[] toArray() {
        ClientMessage request = QueueIteratorParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
        int i = 0;
        Object[] array = new Object[resultCollection.size()];
        for (Data data : resultCollection) {
            array[i++] = toObject(data);
        }
        return array;
    }

    public <T> T[] toArray(T[] ts) {
        ClientMessage request = QueueIteratorParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
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
        ClientMessage request = QueueContainsAllParameters.encode(name, getDataList(c));
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean addAll(Collection<? extends E> c) {
        ClientMessage request = QueueAddAllParameters.encode(name, getDataList(c));
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean removeAll(Collection<?> c) {
        ClientMessage request = QueueCompareAndRemoveAllParameters.encode(name, getDataList(c));
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean retainAll(Collection<?> c) {
        ClientMessage request = QueueCompareAndRetainAllParameters.encode(name, getDataList(c));
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public void clear() {
        ClientMessage request = QueueClearParameters.encode(name);
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
