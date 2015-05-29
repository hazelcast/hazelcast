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
import com.hazelcast.client.impl.protocol.codec.ListAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddAllWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ListAddWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListClearCodec;
import com.hazelcast.client.impl.protocol.codec.ListCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListContainsCodec;
import com.hazelcast.client.impl.protocol.codec.ListGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.ListGetCodec;
import com.hazelcast.client.impl.protocol.codec.ListIndexOfCodec;
import com.hazelcast.client.impl.protocol.codec.ListIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListSetCodec;
import com.hazelcast.client.impl.protocol.codec.ListSizeCodec;
import com.hazelcast.client.impl.protocol.codec.ListSubCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * @author ali 5/20/13
 */
public class ClientListProxy<E> extends ClientProxy implements IList<E> {

    private final String name;

    public ClientListProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        ClientMessage request = ListAddAllWithIndexCodec.encodeRequest(name, index, valueList);
        ClientMessage response = invoke(request);
        ListAddAllWithIndexCodec.ResponseParameters resultParameters = ListAddAllWithIndexCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public E get(int index) {
        ClientMessage request = ListGetCodec.encodeRequest(name, index);
        ClientMessage response = invoke(request);
        ListGetCodec.ResponseParameters resultParameters = ListGetCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public E set(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        ClientMessage request = ListSetCodec.encodeRequest(name, index, value);
        ClientMessage response = invoke(request);
        ListSetCodec.ResponseParameters resultParameters = ListSetCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public void add(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        ClientMessage request = ListAddWithIndexCodec.encodeRequest(name, index, value);
        invoke(request);
    }

    public E remove(int index) {
        ClientMessage request = ListRemoveWithIndexCodec.encodeRequest(name, index);
        ClientMessage response = invoke(request);
        ListRemoveWithIndexCodec.ResponseParameters resultParameters = ListRemoveWithIndexCodec.decodeResponse(response);
        return toObject(resultParameters.response);
    }

    public int size() {
        ClientMessage request = ListSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        ListSizeCodec.ResponseParameters resultParameters = ListSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean isEmpty() {
        ClientMessage request = ListIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        ListIsEmptyCodec.ResponseParameters resultParameters = ListIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListContainsCodec.encodeRequest(name, value);
        ClientMessage response = invoke(request);
        ListContainsCodec.ResponseParameters resultParameters = ListContainsCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public Iterator<E> iterator() {
        return Collections.unmodifiableCollection(getAll()).iterator();
    }

    public Object[] toArray() {
        return getAll().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return getAll().toArray(a);
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final Data element = toData(e);
        ClientMessage request = ListAddCodec.encodeRequest(name, element);
        ClientMessage response = invoke(request);
        ListAddCodec.ResponseParameters resultParameters = ListAddCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListRemoveCodec.encodeRequest(name, value);
        ClientMessage response = invoke(request);
        ListRemoveCodec.ResponseParameters resultParameters = ListRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean containsAll(Collection<?> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListContainsAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invoke(request);
        ListContainsAllCodec.ResponseParameters resultParameters = ListContainsAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean addAll(Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        ClientMessage request = ListAddAllCodec.encodeRequest(name, valueList);
        ClientMessage response = invoke(request);
        ListAddAllCodec.ResponseParameters resultParameters = ListAddAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean removeAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListCompareAndRemoveAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invoke(request);
        ListCompareAndRemoveAllCodec.ResponseParameters resultParameters = ListCompareAndRemoveAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean retainAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListCompareAndRetainAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invoke(request);
        ListCompareAndRetainAllCodec.ResponseParameters resultParameters = ListCompareAndRetainAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public void clear() {
        ClientMessage request = ListClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        ClientMessage request = ListAddListenerCodec.encodeRequest(name, includeValue);

        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(includeValue, listener);
        return listen(request, getPartitionKey(), eventHandler);
    }

    public boolean removeItemListener(String registrationId) {
        ClientMessage request = ListRemoveListenerCodec.encodeRequest(name, registrationId);
        return stopListening(request, registrationId);
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

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getPartitionKey());
    }

    private Collection<E> getAll() {
        ClientMessage request = ListGetAllCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        ListGetAllCodec.ResponseParameters resultParameters = ListGetAllCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        final ArrayList<E> list = new ArrayList<E>(resultCollection.size());
        for (Data value : resultCollection) {
            list.add((E) toObject(value));
        }
        return list;
    }

    public int lastIndexOf(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListLastIndexOfCodec.encodeRequest(name, value);
        ClientMessage response = invoke(request);
        ListLastIndexOfCodec.ResponseParameters resultParameters = ListLastIndexOfCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public int indexOf(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListIndexOfCodec.encodeRequest(name, value);
        ClientMessage response = invoke(request);
        ListIndexOfCodec.ResponseParameters resultParameters = ListIndexOfCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
        return subList(-1, -1).listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        ClientMessage request = ListSubCodec.encodeRequest(name, fromIndex, toIndex);
        ClientMessage response = invoke(request);
        ListSubCodec.ResponseParameters resultParameters = ListSubCodec.decodeResponse(response);
        Collection<Data> resultCollection = resultParameters.list;
        final List<E> list = new ArrayList<E>(resultCollection.size());
        for (Data value : resultCollection) {
            list.add((E) toObject(value));
        }
        return list;
    }

    @Override
    public String toString() {
        return "IList{" + "name='" + getName() + '\'' + '}';
    }
}
