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
import com.hazelcast.client.impl.protocol.parameters.ItemEventParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddAllWithIndexParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddWithIndexParameters;
import com.hazelcast.client.impl.protocol.parameters.ListClearParameters;
import com.hazelcast.client.impl.protocol.parameters.ListCompareAndRemoveAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ListCompareAndRetainAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ListContainsAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ListContainsParameters;
import com.hazelcast.client.impl.protocol.parameters.ListGetAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ListGetParameters;
import com.hazelcast.client.impl.protocol.parameters.ListIndexOfParameters;
import com.hazelcast.client.impl.protocol.parameters.ListIsEmptyParameters;
import com.hazelcast.client.impl.protocol.parameters.ListLastIndexOfParameters;
import com.hazelcast.client.impl.protocol.parameters.ListRemoveListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.ListRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.ListRemoveWithIndexParameters;
import com.hazelcast.client.impl.protocol.parameters.ListSetParameters;
import com.hazelcast.client.impl.protocol.parameters.ListSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.ListSubParameters;
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
        ClientMessage request = ListAddAllWithIndexParameters.encode(name, index, valueList);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public E get(int index) {
        ClientMessage request = ListGetParameters.encode(name, index);
        ClientMessage response = invoke(request);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    public E set(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        ClientMessage request = ListSetParameters.encode(name, index, value);
        ClientMessage response = invoke(request);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    public void add(int index, E element) {
        throwExceptionIfNull(element);
        final Data value = toData(element);
        ClientMessage request = ListAddWithIndexParameters.encode(name, index, value);
        invoke(request);
    }

    public E remove(int index) {
        ClientMessage request = ListRemoveWithIndexParameters.encode(name, index);
        ClientMessage response = invoke(request);
        GenericResultParameters resultParameters = GenericResultParameters.decode(response);
        return toObject(resultParameters.result);
    }

    public int size() {
        ClientMessage request = ListSizeParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean isEmpty() {
        ClientMessage request = ListIsEmptyParameters.encode(name);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListContainsParameters.encode(name, value);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
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
        ClientMessage request = ListAddParameters.encode(name, element);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListRemoveParameters.encode(name, value);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean containsAll(Collection<?> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListContainsAllParameters.encode(name, valueSet);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean addAll(Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        ClientMessage request = ListAddAllParameters.encode(name, valueList);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean removeAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListCompareAndRemoveAllParameters.encode(name, valueSet);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean retainAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = ListCompareAndRetainAllParameters.encode(name, valueSet);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public void clear() {
        ClientMessage request = ListClearParameters.encode(name);
        invoke(request);
    }

    @Override
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        ClientMessage request = ListAddListenerParameters.encode(name, includeValue);

        EventHandler<ClientMessage> eventHandler = new EventHandler<ClientMessage>() {
            final SerializationService serializationService = getContext().getSerializationService();
            final ClientClusterService clusterService = getContext().getClusterService();

            public void handle(ClientMessage message) {
                ItemEventParameters event = ItemEventParameters.decode(message);
                E item = includeValue ? (E) serializationService.toObject(event.item) : null;
                Member member = clusterService.getMember(event.uuid);
                ItemEvent<E> itemEvent = new ItemEvent<E>(name, event.eventType, item, member);
                if (event.eventType == ItemEventType.ADDED) {
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
        ClientMessage request = ListRemoveListenerParameters.encode(name, registrationId);
        return stopListening(request, registrationId);
    }

    protected <T> T invoke(ClientMessage req) {
        return super.invoke(req, getPartitionKey());
    }

    private Collection<E> getAll() {
        ClientMessage request = ListGetAllParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
        final ArrayList<E> list = new ArrayList<E>(resultCollection.size());
        for (Data value : resultCollection) {
            list.add((E) toObject(value));
        }
        return list;
    }

    public int lastIndexOf(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListLastIndexOfParameters.encode(name, value);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public int indexOf(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = ListIndexOfParameters.encode(name, value);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    public ListIterator<E> listIterator(int index) {
        return subList(-1, -1).listIterator(index);
    }

    public List<E> subList(int fromIndex, int toIndex) {
        ClientMessage request = ListSubParameters.encode(name, fromIndex, toIndex);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
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
