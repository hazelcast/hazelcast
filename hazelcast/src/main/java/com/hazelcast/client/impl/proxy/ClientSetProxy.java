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
import com.hazelcast.client.impl.protocol.codec.SetAddAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec;
import com.hazelcast.client.impl.protocol.codec.SetClearCodec;
import com.hazelcast.client.impl.protocol.codec.SetCompareAndRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetCompareAndRetainAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetContainsAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetContainsCodec;
import com.hazelcast.client.impl.protocol.codec.SetGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.SetIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.SetRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.SetRemoveListenerCodec;
import com.hazelcast.client.impl.protocol.codec.SetSizeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.LocalSetStats;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Proxy implementation of {@link ISet}.
 *
 * @param <E> the type of elements in this set
 */
public class ClientSetProxy<E> extends PartitionSpecificClientProxy implements ISet<E> {

    public ClientSetProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    public int size() {
        ClientMessage request = SetSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return SetSizeCodec.decodeResponse(response);
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = SetIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return SetIsEmptyCodec.decodeResponse(response);
    }

    @Override
    public boolean contains(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");
        Data value = toData(o);
        ClientMessage request = SetContainsCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return SetContainsCodec.decodeResponse(response);
    }

    @Override
    public Iterator<E> iterator() {
        return getAll().iterator();
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
        ClientMessage request = SetAddCodec.encodeRequest(name, element);
        ClientMessage response = invokeOnPartition(request);
        return SetAddCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(@Nonnull Object o) {
        checkNotNull(o, "Null item is not allowed");
        Data value = toData(o);
        ClientMessage request = SetRemoveCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        return SetRemoveCodec.decodeResponse(response);
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetContainsAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return SetContainsAllCodec.decodeResponse(response);
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetAddAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return SetAddAllCodec.decodeResponse(response);
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetCompareAndRemoveAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return SetCompareAndRemoveAllCodec.decodeResponse(response);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        checkNotNull(c, "Null collection is not allowed");
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetCompareAndRetainAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        return SetCompareAndRetainAllCodec.decodeResponse(response);
    }

    @Override
    public void clear() {
        ClientMessage request = SetClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Nonnull
    @Override
    public UUID addItemListener(@Nonnull final ItemListener<E> listener, final boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(listener);
        return registerListener(createItemListenerCodec(includeValue), eventHandler);
    }

    private ListenerMessageCodec createItemListenerCodec(final boolean includeValue) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return SetAddListenerCodec.encodeRequest(name, includeValue, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return SetAddListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return SetRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return SetRemoveListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public boolean removeItemListener(@Nonnull UUID registrationId) {
        return deregisterListener(registrationId);
    }

    private Collection<E> getAll() {
        ClientMessage request = SetGetAllCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        return new UnmodifiableLazyList(SetGetAllCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public LocalSetStats getLocalSetStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public String toString() {
        return "ISet{" + "name='" + name + '\'' + '}';
    }

    private class ItemEventHandler extends SetAddListenerCodec.AbstractEventHandler
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
