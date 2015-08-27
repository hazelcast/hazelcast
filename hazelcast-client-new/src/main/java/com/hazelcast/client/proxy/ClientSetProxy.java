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

import com.hazelcast.client.impl.ClientMessageDecoder;
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
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ClientSetProxy<E> extends PartitionSpecificClientProxy implements ISet<E> {

    public ClientSetProxy(String serviceName, String name) {
        super(serviceName, name);
    }

    @Override
    public int size() {
        ClientMessage request = SetSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SetSizeCodec.ResponseParameters resultParameters = SetSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean isEmpty() {
        ClientMessage request = SetIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SetIsEmptyCodec.ResponseParameters resultParameters = SetIsEmptyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean contains(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = SetContainsCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        SetContainsCodec.ResponseParameters resultParameters = SetContainsCodec.decodeResponse(response);
        return resultParameters.response;
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
        final Data element = toData(e);
        ClientMessage request = SetAddCodec.encodeRequest(name, element);
        ClientMessage response = invokeOnPartition(request);
        SetAddCodec.ResponseParameters resultParameters = SetAddCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean remove(Object o) {
        throwExceptionIfNull(o);
        final Data value = toData(o);
        ClientMessage request = SetRemoveCodec.encodeRequest(name, value);
        ClientMessage response = invokeOnPartition(request);
        SetRemoveCodec.ResponseParameters resultParameters = SetRemoveCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean containsAll(Collection<?> c) {
        throwExceptionIfNull(c);
        Set<Data> valueSet = new HashSet<Data>(c.size());
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = SetContainsAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invokeOnPartition(request);
        SetContainsAllCodec.ResponseParameters resultParameters = SetContainsAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean addAll(Collection<? extends E> c) {
        throwExceptionIfNull(c);
        final List<Data> valueList = new ArrayList<Data>(c.size());
        for (E e : c) {
            throwExceptionIfNull(e);
            valueList.add(toData(e));
        }
        ClientMessage request = SetAddAllCodec.encodeRequest(name, valueList);
        ClientMessage response = invokeOnPartition(request);
        SetAddAllCodec.ResponseParameters resultParameters = SetAddAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean removeAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = SetCompareAndRemoveAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invokeOnPartition(request);
        SetCompareAndRemoveAllCodec.ResponseParameters resultParameters = SetCompareAndRemoveAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public boolean retainAll(Collection<?> c) {
        throwExceptionIfNull(c);
        final Set<Data> valueSet = new HashSet<Data>();
        for (Object o : c) {
            throwExceptionIfNull(o);
            valueSet.add(toData(o));
        }
        ClientMessage request = SetCompareAndRetainAllCodec.encodeRequest(name, valueSet);
        ClientMessage response = invokeOnPartition(request);
        SetCompareAndRetainAllCodec.ResponseParameters resultParameters = SetCompareAndRetainAllCodec.decodeResponse(response);
        return resultParameters.response;
    }

    public void clear() {
        ClientMessage request = SetClearCodec.encodeRequest(name);
        invokeOnPartition(request);
    }

    public String addItemListener(final ItemListener<E> listener, final boolean includeValue) {
        ClientMessage request = SetAddListenerCodec.encodeRequest(name, includeValue);

        EventHandler<ClientMessage> eventHandler = new ItemEventHandler(includeValue, listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) SetAddListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, getPartitionKey(), eventHandler, responseDecoder);
    }

    public boolean removeItemListener(String registrationId) {
        return stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return SetRemoveListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return SetRemoveListenerCodec.decodeResponse(clientMessage).response;
            }
        });
    }

    private Collection<E> getAll() {
        ClientMessage request = SetGetAllCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request);
        SetGetAllCodec.ResponseParameters resultParameters = SetGetAllCodec.decodeResponse(response);
        List<Data> resultCollection = resultParameters.list;
        SerializationService serializationService = getContext().getSerializationService();
        return new UnmodifiableLazyList<E>(resultCollection, serializationService);
    }

    @Override
    public String toString() {
        return "ISet{" + "name='" + name + '\'' + '}';
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

}
