/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
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
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.Preconditions.isNotNull;

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
        SetSizeCodec.ResponseParameters resultParameters = SetSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = SetIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SetIsEmptyCodec.ResponseParameters resultParameters = SetIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean contains(Object o) {
        Preconditions.checkNotNull(o);
        Data value = toData(o);
        ClientMessage request = SetContainsCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        SetContainsCodec.ResponseParameters resultParameters = SetContainsCodec.decodeResponse(response);
        return resultParameters.response;
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
    public <T> T[] toArray(T[] a) {
        return getAll().toArray(a);
    }

    @Override
    public boolean add(E e) {
        Preconditions.checkNotNull(e);
        Data element = toData(e);
        ClientMessage request = SetAddCodec.encodeRequest(name, element);
        ClientMessage response = invokeOnPartition(request);
        SetAddCodec.ResponseParameters resultParameters = SetAddCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean remove(Object o) {
        Preconditions.checkNotNull(o);
        Data value = toData(o);
        ClientMessage request = SetRemoveCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        SetRemoveCodec.ResponseParameters resultParameters = SetRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        Preconditions.checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetContainsAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        SetContainsAllCodec.ResponseParameters resultParameters = SetContainsAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        Preconditions.checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetAddAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        SetAddAllCodec.ResponseParameters resultParameters = SetAddAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        Preconditions.checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetCompareAndRemoveAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        SetCompareAndRemoveAllCodec.ResponseParameters resultParameters = SetCompareAndRemoveAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Preconditions.checkNotNull(c);
        Collection<Data> dataCollection = objectToDataCollection(c, getSerializationService());
        ClientMessage request = SetCompareAndRetainAllCodec.encodeRequest(name, dataCollection);
        ClientMessage response = invokeOnPartition(request);
        SetCompareAndRetainAllCodec.ResponseParameters resultParameters = SetCompareAndRetainAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void clear() {
        ClientMessage request = SetClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    @Override
    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        isNotNull(listener, "listener");
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
            public String decodeAddResponse(ClientMessage clientMessage) {
                return SetAddListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return SetRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return SetRemoveListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public boolean removeItemListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    private Collection<E> getAll() {
        ClientMessage request = SetGetAllCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SetGetAllCodec.ResponseParameters resultParameters = SetGetAllCodec.decodeResponse(response);
        List<Data> resultCollection = resultParameters.response;
        return new UnmodifiableLazyList<E>(resultCollection, getSerializationService());
    }

    @Override
    public String toString() {
        return "ISet{" + "name='" + name + '\'' + '}';
    }

    private class ItemEventHandler extends ListAddListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ItemListener<E> listener;

        public ItemEventHandler(ItemListener<E> listener) {
            this.listener = listener;
        }

        @Override
        public void handle(Data dataItem, String uuid, int eventType) {
            Member member = getContext().getClusterService().getMember(uuid);
            ItemEvent<E> itemEvent = new DataAwareItemEvent(name, ItemEventType.getByType(eventType),
                    dataItem, member, getSerializationService());
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
}
