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
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.client.*;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableItemEvent;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;

import java.util.*;

/**
 * @author ali 5/20/13
 */
public class ClientListProxy<E> extends ClientProxy implements IList<E> {

    private final CollectionProxyId proxyId;

    private volatile Data key;

    public ClientListProxy(String serviceName, CollectionProxyId objectId) {
        super(serviceName, objectId);
        proxyId = objectId;
    }

    public String addItemListener(ItemListener<E> listener, boolean includeValue) {
        AddItemListenerRequest request = new AddItemListenerRequest(proxyId, getKey(), includeValue);
        EventHandler<PortableItemEvent> handler = createHandler(listener, includeValue);
        return listen(request, getKey(), handler);
    }

    public boolean removeItemListener(String registrationId) {
        return stopListening(registrationId);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        List<Data> list = new ArrayList<Data>(c.size());
        for (E e : c) {
            Data data = getSerializationService().toData(e);
            list.add(data);
        }
        AddAllRequest request = new AddAllRequest(proxyId, getKey(), ThreadUtil.getThreadId(), list, index);
        Boolean result = invoke(request);
        return result;
    }

    public E get(int index) {
        GetRequest request = new GetRequest(proxyId, getKey(), index);
        return invoke(request);
    }

    public E set(int index, E element) {
        Data valueData = getSerializationService().toData(element);
        SetRequest request = new SetRequest(proxyId, getKey(), valueData, index, ThreadUtil.getThreadId());
        return invoke(request);
    }

    public void add(int index, E element) {
        Data valueData = getSerializationService().toData(element);
        PutRequest request = new PutRequest(proxyId, getKey(), valueData, index, ThreadUtil.getThreadId());
        invoke(request);
    }

    public E remove(int index) {
        RemoveIndexRequest request = new RemoveIndexRequest(proxyId, getKey(), index, ThreadUtil.getThreadId());
        return invoke(request);
    }

    public int indexOf(Object o) {
        Data valueData = getSerializationService().toData(o);
        IndexOfRequest request = new IndexOfRequest(proxyId, getKey(), valueData, false);
        Integer result = invoke(request);
        return result;
    }

    public int lastIndexOf(Object o) {
        Data valueData = getSerializationService().toData(o);
        IndexOfRequest request = new IndexOfRequest(proxyId, getKey(), valueData, true);
        Integer result = invoke(request);
        return result;
    }

    public ListIterator<E> listIterator() {
        return getList().listIterator();
    }

    public ListIterator<E> listIterator(int index) {
        return getList().listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return getList().subList(fromIndex, toIndex);
    }

    public int size() {
        CountRequest request =  new CountRequest(proxyId, getKey());
        Integer result = invoke(request);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(Object o) {
        Data valueData = getSerializationService().toData(o);
        ContainsEntryRequest request = new ContainsEntryRequest(proxyId, getKey(), valueData);
        Boolean result = invoke(request);
        return result;
    }

    public Iterator<E> iterator() {
        return getList().iterator();
    }

    public Object[] toArray() {
        return getList().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return getList().toArray(a);
    }

    public boolean add(E e) {
        Data valueData = getSerializationService().toData(e);
        PutRequest request = new PutRequest(proxyId, getKey(), valueData, -1, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    public boolean remove(Object o) {
        Data valueData = getSerializationService().toData(o);
        RemoveRequest request = new RemoveRequest(proxyId, getKey(), valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    public boolean containsAll(Collection<?> c) {
        Set<Data> set = new HashSet<Data>(c.size());
        for (Object o : c) {
            Data data = getSerializationService().toData(o);
            set.add(data);
        }
        ContainsAllRequest request = new ContainsAllRequest(proxyId, getKey(), set);
        Boolean result = invoke(request);
        return result;
    }

    public boolean addAll(Collection<? extends E> c) {
        List<Data> list = new ArrayList<Data>(c.size());
        for (E e : c) {
            Data data = getSerializationService().toData(e);
            list.add(data);
        }
        AddAllRequest request = new AddAllRequest(proxyId, getKey(), ThreadUtil.getThreadId(), list);
        Boolean result = invoke(request);
        return result;
    }

    public boolean removeAll(Collection<?> c) {
        List<Data> list = new ArrayList<Data>(c.size());
        for (Object o : c) {
            Data data = getSerializationService().toData(o);
            list.add(data);
        }
        CompareAndRemoveRequest request = new CompareAndRemoveRequest(proxyId, getKey(), list, false, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    public boolean retainAll(Collection<?> c) {
        List<Data> list = new ArrayList<Data>(c.size());
        for (Object o : c) {
            Data data = getSerializationService().toData(o);
            list.add(data);
        }
        CompareAndRemoveRequest request = new CompareAndRemoveRequest(proxyId, getKey(), list, true, ThreadUtil.getThreadId());
        Boolean result = invoke(request);
        return result;
    }

    public void clear() {
        RemoveAllRequest request = new RemoveAllRequest(proxyId, getKey(), ThreadUtil.getThreadId());
        invoke(request);
    }

    protected void onDestroy() {
        CollectionDestroyRequest request = new CollectionDestroyRequest(proxyId);
        invoke(request);
    }

    public String getName() {
        return proxyId.getKeyName();
    }

    private <T> T invoke(Object req) {
        try {
            return getContext().getInvocationService().invokeOnKeyOwner(req, getKey());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Data getKey(){
        if (key == null){
            key = getSerializationService().toData(proxyId.getKeyName());
        }
        return key;
    }

    private List<E> getList(){
        GetAllRequest request = new GetAllRequest(proxyId, getKey());
        PortableCollection result = invoke(request);
        Collection<Data> collection = result.getCollection();
        List<E> list = new ArrayList<E>(collection.size());
        for (Data data : collection) {
            list.add((E) getSerializationService().toObject(data));
        }
        return list;
    }

    private SerializationService getSerializationService(){
        return getContext().getSerializationService();
    }

    private EventHandler<PortableItemEvent> createHandler(final ItemListener<E> listener, final boolean includeValue){
        return new EventHandler<PortableItemEvent>() {
            public void handle(PortableItemEvent event) {
                E item = null;
                if (includeValue){
                    item = (E)getSerializationService().toObject(event.getItem());
                }
                Member member = getContext().getClusterService().getMember(event.getUuid());
                ItemEvent<E> itemEvent = new ItemEvent<E>(proxyId.getKeyName(), event.getEventType(), item, member);

                switch (event.getEventType()){
                    case ADDED:
                        listener.itemAdded(itemEvent);
                        break;
                    case REMOVED:
                        listener.itemRemoved(itemEvent);
                        break;
                }
            }
        };
    }
}
