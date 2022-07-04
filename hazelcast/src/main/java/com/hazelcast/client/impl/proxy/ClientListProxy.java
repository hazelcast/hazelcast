/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

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
import com.hazelcast.client.impl.protocol.codec.ListIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.ListLastIndexOfCodec;
import com.hazelcast.client.impl.protocol.codec.ListListIteratorCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ListRemoveWithIndexCodec;
import com.hazelcast.client.impl.protocol.codec.ListSetCodec;
import com.hazelcast.client.impl.protocol.codec.ListSizeCodec;
import com.hazelcast.client.impl.protocol.codec.ListSubCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.LocalListStats;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link IList}.
 *
 * @param <E> the type of elements in this list
 */
public class ClientListProxy<E> extends PartitionSpecificClientProxy implements IList<E> {

    public ClientListProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    public boolean addAll(int index, @Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = ListAddAllWithIndexCodec.encodeRequest(name, index, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return ListAddAllWithIndexCodec.decodeResponse(response);
    }

    @Override
    public E get(int index) {
        ClientMessage request = ListGetCodec.encodeRequest(name, index);
        ClientMessage response = invokeOnPartition(request);
        return toObject(ListGetCodec.decodeResponse(response));
    }

    @Override
    public E set(int index, @Nonnull E element) {
        checkNotNull(element, "Null item is not allowed");
        Data value = toData(element);
        ClientMessage request = ListSetCodec.encodeRequest(name, index, value);
        ClientMessage response = invokeOnPartition(request);
        return toObject(ListSetCodec.decodeResponse(response));
    }

    @Override
    public void add(int index, @Nonnull E element) {
        checkNotNull(element, "Null item is not allowed");
        Data value = toData(element);
        ClientMessage request = ListAddWithIndexCodec.encodeRequest(name, index, value);
        invokeOnPartition(request);
    }

    @Override
    public E remove(int index) {
        ClientMessage request = ListRemoveWithIndexCodec.encodeRequest(name, index);
        ClientMessage response = invokeOnPartition(request);
        return toObject(ListRemoveWithIndexCodec.decodeResponse(response));
    }

    @Override
    public int size() {
        ClientMessage request = ListSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return ListSizeCodec.decodeResponse(response);
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ListIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return ListIsEmptyCodec.decodeResponse(response);
    }

    @Override
    public boolean contains(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");
        Data value = toData(o);
        ClientMessage request = ListContainsCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return ListContainsCodec.decodeResponse(response);
    }

    @Override
    public Iterator<E> iterator() {
        ClientMessage request = ListIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        List<Data> resultCollection = ListIteratorCodec.decodeResponse(response);
        return (Iterator<E>) new UnmodifiableLazyList(resultCollection, getSerializationService()).iterator();
    }

    @Override
    public Object[] toArray() {
        return getAll().toArray();
    }

    @Override
    public <T> T[] toArray(@Nonnull T[] a) {
        checkNotNull(a, "Null array parameter is not allowed!");
        return getAll().toArray(a);
    }

    @Override
    public boolean add(@Nonnull E e) {
        checkNotNull(e, "Null item is not allowed");
        Data element = toData(e);
        ClientMessage request = ListAddCodec.encodeRequest(name, element);
        ClientMessage response = invokeOnPartition(request);
        return ListAddCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");
        Data value = toData(o);
        ClientMessage request = ListRemoveCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return ListRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = ListContainsAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return ListContainsAllCodec.decodeResponse(response);
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = ListAddAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return ListAddAllCodec.decodeResponse(response);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = ListCompareAndRemoveAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return
                ListCompareAndRemoveAllCodec.decodeResponse(response);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = ListCompareAndRetainAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return
                ListCompareAndRetainAllCodec.decodeResponse(response);
    }

    @Override
    public void clear() {
        ClientMessage request = ListClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Nonnull
    @Override
    public UUID addItemListener(@Nonnull ItemListener<E> listener, boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(listener);
        return registerListener(createItemListenerCodec(includeValue), eventHandler);
    }

    private ListenerMessageCodec createItemListenerCodec(final boolean includeValue) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ListAddListenerCodec.encodeRequest(name, includeValue, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ListAddListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ListRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ListRemoveListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public boolean removeItemListener(@Nonnull UUID registrationId) {
        return deregisterListener(registrationId);
    }

    private Collection<E> getAll() {
        ClientMessage request = ListGetAllCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return new UnmodifiableLazyList(ListGetAllCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public int lastIndexOf(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");
        Data value = toData(o);
        ClientMessage request = ListLastIndexOfCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return ListLastIndexOfCodec.decodeResponse(response);
    }

    @Override
    public int indexOf(Object o) {
        checkNotNull(o);
        Data value = toData(o);
        ClientMessage request = ListIndexOfCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return ListIndexOfCodec.decodeResponse(response);
    }

    @Override
    public ListIterator<E> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        ClientMessage request = ListListIteratorCodec.encodeRequest(name, index);
        ClientMessage response = invokeOnPartition(request);
        List<Data> resultCollection = ListListIteratorCodec.decodeResponse(response);
        return (ListIterator<E>) new UnmodifiableLazyList(resultCollection, getSerializationService()).listIterator();
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        ClientMessage request = ListSubCodec.encodeRequest(name, fromIndex, toIndex);
        ClientMessage response = invokeOnPartition(request);
        return new UnmodifiableLazyList(ListSubCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public LocalListStats getLocalListStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String toString() {
        return "IList{" + "name='" + name + '\'' + '}';
    }

    // used by jet
    public Iterator<Data> dataIterator() {
        ClientMessage request = ListIteratorCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return Collections.unmodifiableList(ListIteratorCodec.decodeResponse(response)).iterator();
    }

    // used by jet
    public List<Data> dataSubList(int fromIndex, int toIndex) {
        ClientMessage request = ListSubCodec.encodeRequest(name, fromIndex, toIndex);
        ClientMessage response = invokeOnPartition(request);
        return Collections.unmodifiableList(ListSubCodec.decodeResponse(response));
    }

    private class ItemEventHandler extends ListAddListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ItemListener<E> listener;

        ItemEventHandler(ItemListener<E> listener) {
            this.listener = listener;
        }

        @Override
        public void handleItemEvent(Data dataItem, UUID uuid, int eventType) {
            Member member = getContext().getClusterService().getMember(uuid);
            ItemEvent<E> itemEvent = new DataAwareItemEvent(name, ItemEventType.getByType(eventType),
                    dataItem, member, getSerializationService());
            if (eventType == ItemEventType.ADDED.getType()) {
                listener.itemAdded(itemEvent);
            } else {
                listener.itemRemoved(itemEvent);
            }
        }
    }
}
