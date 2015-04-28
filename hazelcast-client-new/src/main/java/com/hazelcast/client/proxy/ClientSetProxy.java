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

import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.SetAddAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetAddParameters;
import com.hazelcast.client.impl.protocol.parameters.SetClearParameters;
import com.hazelcast.client.impl.protocol.parameters.SetCompareAndRemoveAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetCompareAndRetainAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetContainsAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetContainsParameters;
import com.hazelcast.client.impl.protocol.parameters.SetGetAllParameters;
import com.hazelcast.client.impl.protocol.parameters.SetIsEmptyParameters;
import com.hazelcast.client.impl.protocol.parameters.SetRemoveListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.SetRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.SetSizeParameters;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.collection.impl.collection.client.CollectionAddListenerRequest;
import com.hazelcast.collection.impl.collection.client.CollectionRequest;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author ali 5/20/13
 */
public class ClientSetProxy<E> extends ClientProxy implements ISet<E> {

    private final String name;

    public ClientSetProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    @Override
    public int size() {
        ClientMessage request = SetSizeParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean isEmpty() {
        ClientMessage request = SetIsEmptyParameters.encode(name);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = SetContainsParameters.encode(name, value);
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
        ClientMessage request = SetAddParameters.encode(name, element);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = SetRemoveParameters.encode(name, value);
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
        ClientMessage request = SetContainsAllParameters.encode(name, valueSet);
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
        ClientMessage request = SetAddAllParameters.encode(name, valueList);
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
        ClientMessage request = SetCompareAndRemoveAllParameters.encode(name, valueSet);
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
        ClientMessage request = SetCompareAndRetainAllParameters.encode(name, valueSet);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public void clear() {
        ClientMessage request = SetClearParameters.encode(name);
        invoke(request);
    }

    //Todo MessageTask!!
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        final CollectionAddListenerRequest request = new CollectionAddListenerRequest(getName(), includeValue);
        request.setServiceName(getServiceName());
        EventHandler<PortableItemEvent> eventHandler = new EventHandler<PortableItemEvent>() {
            public void handle(PortableItemEvent portableItemEvent) {
                E item = includeValue ? (E) getContext().getSerializationService().toObject(portableItemEvent.getItem()) : null;
                Member member = getContext().getClusterService().getMember(portableItemEvent.getUuid());
                ItemEvent<E> itemEvent = new ItemEvent<E>(getName(), portableItemEvent.getEventType(), item, member);
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
        ClientMessage request = SetRemoveListenerParameters.encode(name, registrationId);
        return stopListening(request, registrationId);
    }

    protected <T> T invoke(ClientRequest req) {
        if (req instanceof CollectionRequest) {
            CollectionRequest request = (CollectionRequest) req;
            request.setServiceName(getServiceName());
        }

        return super.invoke(req, getPartitionKey());
    }

    private Collection<E> getAll() {
        ClientMessage request = SetGetAllParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> resultCollection = resultParameters.result;
        final ArrayList<E> list = new ArrayList<E>(resultCollection.size());
        for (Data value : resultCollection) {
            list.add((E) toObject(value));
        }
        return list;
    }

    @Override
    public String toString() {
        return "ISet{" + "name='" + getName() + '\'' + '}';
    }

}
