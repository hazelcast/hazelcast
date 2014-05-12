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
import com.hazelcast.collection.client.CollectionAddAllRequest;
import com.hazelcast.collection.client.CollectionClearRequest;
import com.hazelcast.collection.client.CollectionSizeRequest;
import com.hazelcast.collection.client.CollectionContainsRequest;
import com.hazelcast.collection.client.CollectionAddRequest;
import com.hazelcast.collection.client.CollectionRemoveRequest;
import com.hazelcast.collection.client.CollectionCompareAndRemoveRequest;
import com.hazelcast.collection.client.CollectionAddListenerRequest;
import com.hazelcast.collection.client.CollectionRemoveListenerRequest;
import com.hazelcast.collection.client.CollectionRequest;
import com.hazelcast.collection.client.CollectionGetAllRequest;

import com.hazelcast.core.ICollection;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableItemEvent;
import com.hazelcast.spi.impl.SerializableCollection;

import java.util.Iterator;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/**
 * @ali 9/4/13
 */
public class AbstractClientCollectionProxy<E> extends ClientProxy implements ICollection<E> {

    protected final String partitionKey;

    public AbstractClientCollectionProxy(String instanceName, String serviceName, String name) {
        super(instanceName, serviceName, name);
        partitionKey = getPartitionKey();
    }

    public int size() {
        CollectionSizeRequest request = new CollectionSizeRequest(getName());
        final Integer result = invoke(request);
        return result;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        final CollectionContainsRequest request = new CollectionContainsRequest(getName(), toData(o));
        final Boolean result = invoke(request);
        return result;
    }

    public Iterator<E> iterator() {
        return getAll().iterator();
    }

    public Object[] toArray() {
        return getAll().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return getAll().toArray(a);
    }

    public boolean add(E e) {
        throwExceptionIfNull(e);
        final CollectionAddRequest request = new CollectionAddRequest(getName(), toData(e));
        final Boolean result = invoke(request);
        return result;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final CollectionRemoveRequest request = new CollectionRemoveRequest(getName(), toData(o));
        final Boolean result = invoke(request);
        return result;
    }

    public boolean containsAll(Collection<?> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        final CollectionContainsRequest request = new CollectionContainsRequest(getName(), valueSet);
        final Boolean result = invoke(request);
        return result;
    }

    public boolean addAll(Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        final CollectionAddAllRequest request = new CollectionAddAllRequest(getName(), valueList);
        final Boolean result = invoke(request);
        return result;
    }

    public boolean removeAll(Collection<?> c) {
        return compareAndRemove(false, c);
    }

    public boolean retainAll(Collection<?> c) {
        return compareAndRemove(true, c);
    }

    private boolean compareAndRemove(boolean retain, Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        final CollectionCompareAndRemoveRequest request = new CollectionCompareAndRemoveRequest(getName(), valueSet, retain);
        final Boolean result = invoke(request);
        return result;
    }

    public void clear() {
        final CollectionClearRequest request = new CollectionClearRequest(getName());
        invoke(request);
    }

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
        };
        return listen(request, getPartitionKey(), eventHandler);
    }

    public boolean removeItemListener(String registrationId) {
        final CollectionRemoveListenerRequest request = new CollectionRemoveListenerRequest(getName(),
                registrationId, getServiceName());
        return stopListening(request, registrationId);
    }

    protected void onDestroy() {
    }

    protected <T> T invoke(ClientRequest req) {
        if (req instanceof CollectionRequest) {
            CollectionRequest request = (CollectionRequest) req;
            request.setServiceName(getServiceName());
        }

        return super.invoke(req, getPartitionKey());
    }

    private Collection<E> getAll() {
        final CollectionGetAllRequest request = new CollectionGetAllRequest(getName());
        final SerializableCollection result = invoke(request);
        final Collection<Data> collection = result.getCollection();
        final ArrayList<E> list = new ArrayList<E>(collection.size());
        for (Data value : collection) {
            list.add((E) toObject(value));
        }
        return list;
    }

}
