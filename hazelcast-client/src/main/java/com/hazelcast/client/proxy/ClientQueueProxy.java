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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.*;
import com.hazelcast.map.client.MapDestroyRequest;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.client.*;
import com.hazelcast.queue.proxy.QueueIterator;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableItemEvent;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @ali 5/19/13
 */
public final class ClientQueueProxy<E> extends ClientProxy implements IQueue<E>{

    private final String name;
    private Data key;

    public ClientQueueProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        final AddListenerRequest request = new AddListenerRequest(name, includeValue);
        EventHandler<PortableItemEvent> eventHandler = new EventHandler<PortableItemEvent>() {
            public void handle(PortableItemEvent portableItemEvent) {
                E item = includeValue ? (E)getContext().getSerializationService().toObject(portableItemEvent.getItem()) : null;
                Member member = getContext().getClusterService().getMember(portableItemEvent.getUuid());
                ItemEvent<E> itemEvent = new ItemEvent<E>(name, portableItemEvent.getEventType(), item, member);
                if (portableItemEvent.getEventType() == ItemEventType.ADDED){
                    listener.itemAdded(itemEvent);
                } else {
                    listener.itemRemoved(itemEvent);
                }
            }
        };
        return listen(request, getKey(), eventHandler);
    }

    public boolean removeItemListener(String registrationId) {
        return stopListening(registrationId);
    }

    public LocalQueueStats getLocalQueueStats() {
        //TODO stats request
        return null;
    }

    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

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
        final Boolean result = invoke(request);
        return result;
    }

    public E take() throws InterruptedException {
        return poll(-1, TimeUnit.MILLISECONDS);
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        PollRequest request = new PollRequest(name, unit.toMillis(timeout));
        return invoke(request);
    }

    public int remainingCapacity() {
        //TODO we may need a request here, since QueueConfig is not accessible
        return 0;
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
            E e = (E)getContext().getSerializationService().toObject(data);
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
        return size() == 0;
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
            ts[i++] = (T)getContext().getSerializationService().toObject(data);
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

    protected void onDestroy() {
        MapDestroyRequest request = new MapDestroyRequest(name);
        invoke(request);
    }

    public String getName() {
        return name;
    }

    private <T> T invoke(Object req){
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Data getKey(){
        if (key == null){
            key = getContext().getSerializationService().toData(name);
        }
        return key;
    }

    private List<Data> getDataList(Collection<?> objects) {
        List<Data> dataList = new ArrayList<Data>(objects.size());
        for (Object o : objects) {
            dataList.add(getContext().getSerializationService().toData(o));
        }
        return dataList;
    }
}
