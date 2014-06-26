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

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.IQueue;

import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.client.AddListenerRequest;
import com.hazelcast.queue.client.IsEmptyRequest;
import com.hazelcast.queue.client.RemoveListenerRequest;
import com.hazelcast.queue.client.RemoveRequest;
import com.hazelcast.queue.client.PollRequest;
import com.hazelcast.queue.client.OfferRequest;
import com.hazelcast.queue.client.RemainingCapacityRequest;
import com.hazelcast.queue.client.PeekRequest;
import com.hazelcast.queue.client.ContainsRequest;
import com.hazelcast.queue.client.DrainRequest;
import com.hazelcast.queue.client.SizeRequest;
import com.hazelcast.queue.client.IteratorRequest;
import com.hazelcast.queue.client.CompareAndRemoveRequest;
import com.hazelcast.queue.client.AddAllRequest;
import com.hazelcast.queue.client.ClearRequest;
import com.hazelcast.queue.proxy.QueueIterator;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author ali 5/19/13
 */
public final class ClientQueueProxy<E> extends ClientProxy implements IQueue<E> {

    private final String name;

    public ClientQueueProxy(String instanceName, String serviceName, String name) {
        super(instanceName, serviceName, name);
        this.name = name;
    }

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
            public void onListenerRegister() {

            }
        };
        return listen(request, getPartitionKey(), eventHandler);
    }

    public boolean removeItemListener(String registrationId) {
        final RemoveListenerRequest request = new RemoveListenerRequest(name, registrationId);
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
        Data data = getContext().getSerializationService().toData(e);
        OfferRequest request = new OfferRequest(name, unit.toMillis(timeout), data);
        final Boolean result = invokeInterruptibly(request);
        return result;
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        PollRequest request = new PollRequest(name, unit.toMillis(timeout));
        return invokeInterruptibly(request);
    }

    public int remainingCapacity() {
        RemainingCapacityRequest request = new RemainingCapacityRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public boolean remove(Object o) {
        Data data = getContext().getSerializationService().toData(o);
        RemoveRequest request = new RemoveRequest(name, data);
        Boolean result = invoke(request);
        return result;
    }

    public boolean contains(Object o) {
        final Collection<Data> list = new ArrayList<Data>(1);
        list.add(getContext().getSerializationService().toData(o));
        ContainsRequest request = new ContainsRequest(name, list);
        Boolean result = invoke(request);
        return result;
    }

    public int drainTo(Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        DrainRequest request = new DrainRequest(name, maxElements);
        PortableCollection result = invoke(request);
        Collection<Data> coll = result.getCollection();
        for (Data data : coll) {
            E e = getContext().getSerializationService().toObject(data);
            c.add(e);
        }
        return coll.size();
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
        PeekRequest request = new PeekRequest(name);
        return invoke(request);
    }

    public int size() {
        SizeRequest request = new SizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public boolean isEmpty() {
        IsEmptyRequest request = new IsEmptyRequest(name);
        Boolean result = invoke(request);
        return result;
    }

    public Iterator<E> iterator() {
        IteratorRequest request = new IteratorRequest(name);
        PortableCollection result = invoke(request);
        Collection<Data> coll = result.getCollection();
        return new QueueIterator<E>(coll.iterator(), getContext().getSerializationService(), false);
    }

    public Object[] toArray() {
        IteratorRequest request = new IteratorRequest(name);
        PortableCollection result = invoke(request);
        Collection<Data> coll = result.getCollection();
        int i = 0;
        Object[] array = new Object[coll.size()];
        for (Data data : coll) {
            array[i++] = getContext().getSerializationService().toObject(data);
        }
        return array;
    }

    public <T> T[] toArray(T[] ts) {
        IteratorRequest request = new IteratorRequest(name);
        PortableCollection result = invoke(request);
        Collection<Data> coll = result.getCollection();
        int size = coll.size();
        if (ts.length < size) {
            ts = (T[]) java.lang.reflect.Array.newInstance(ts.getClass().getComponentType(), size);
        }
        int i = 0;
        for (Data data : coll) {
            ts[i++] = (T) getContext().getSerializationService().toObject(data);
        }
        return ts;
    }

    public boolean containsAll(Collection<?> c) {
        List<Data> list = getDataList(c);
        ContainsRequest request = new ContainsRequest(name, list);
        Boolean result = invoke(request);
        return result;
    }

    public boolean addAll(Collection<? extends E> c) {
        AddAllRequest request = new AddAllRequest(name, getDataList(c));
        Boolean result = invoke(request);
        return result;
    }

    public boolean removeAll(Collection<?> c) {
        CompareAndRemoveRequest request = new CompareAndRemoveRequest(name, getDataList(c), false);
        Boolean result = invoke(request);
        return result;
    }

    public boolean retainAll(Collection<?> c) {
        CompareAndRemoveRequest request = new CompareAndRemoveRequest(name, getDataList(c), true);
        Boolean result = invoke(request);
        return result;
    }

    public void clear() {
        ClearRequest request = new ClearRequest(name);
        invoke(request);
    }

    protected <T> T invoke(ClientRequest req) {
        return super.invoke(req, getPartitionKey());
    }

    protected <T> T invokeInterruptibly(ClientRequest req) throws InterruptedException {
        return super.invokeInterruptibly(req, getPartitionKey());
    }

    private List<Data> getDataList(Collection<?> objects) {
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(getContext().getSerializationService().toData(o));
        }
        return dataList;
    }

    @Override
    public String toString() {
        return "IQueue{" + "name='" + getName() + '\'' + '}';
    }
}
