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

import com.hazelcast.client.CollectionClientProxy;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ItemEventLRH;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.client.proxy.ProxyHelper.check;

public class ListClientProxy<E> extends CollectionClientProxy<E> implements IList<E> {
    private Map<ItemListener, ListenerThread> listenerMap = new ConcurrentHashMap<ItemListener, ListenerThread>();

    public ListClientProxy(HazelcastClient hazelcastClient, String name) {
        super(hazelcastClient, name);
    }

    @Override
    public boolean add(E o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.LADD, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public boolean remove(Object o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.LREMOVE, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public int size() {
        return proxyHelper.doCommandAsInt(Command.LSIZE, new String[]{getName()});
    }

    @Override
    public boolean contains(Object o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.LCONTAINS, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IList && o != null) {
            return getName().equals(((IList) o).getName());
        } else {
            return false;
        }
    }

    @Override
    protected List<E> getTheCollection() {
        return proxyHelper.doCommandAsList(Command.LGETALL, new String[]{getName()});
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{ObjectListProxy.COLLECTION_LIST_NAME, getName()});
    }

    public void add(int index, E element) {
        String[] args = new String[]{getName(), String.valueOf(index)};
        proxyHelper.doCommand(Command.LADD, args, proxyHelper.toData(element));
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        String[] args = new String[]{getName(), String.valueOf(index)};
        List<Data> list = new ArrayList<Data>();
        for(E e: c){
            list.add(proxyHelper.toData(e));
        }
        return proxyHelper.doCommandAsBoolean(Command.LCONTAINS, args, list.toArray(new Data[]{}));
    }

    public E get(int index) {
        return (E)proxyHelper.doCommandAsObject(Command.LGET, new String[]{getName(), String.valueOf(index)});
    }

    public int indexOf(Object o) {
        check(o);
        return proxyHelper.doCommandAsInt(Command.LINDEXOF, new String[]{getName()}, proxyHelper.toData(o));
    }

    public int lastIndexOf(Object o) {
        check(o);
        return proxyHelper.doCommandAsInt(Command.LLASTINDEXOF, new String[]{getName()}, proxyHelper.toData(o));
    }

    public ListIterator<E> listIterator() {
        return getTheCollection().listIterator();
    }

    public ListIterator<E> listIterator(int index) {
        return getTheCollection().listIterator(index);
    }

    public E remove(int index) {
        return (E) proxyHelper.doCommandAsObject(Command.LREMOVE, new String[]{getName(), String.valueOf(index)});
    }

    public E set(int index, E element) {
        String[] args = new String[]{getName(), String.valueOf(index)};
        return (E) proxyHelper.doCommandAsObject(Command.LSET, args, proxyHelper.toData(element));
    }

    public List<E> subList(int fromIndex, int toIndex) {
        return getTheCollection().subList(fromIndex, toIndex);
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        Protocol request = proxyHelper.createProtocol(Command.SLISTEN, new String[]{getName(), String.valueOf(includeValue)}, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.listListener.",
                client, request, new ItemEventLRH<E>(listener, includeValue, this));
        listenerMap.put(listener, thread);
        thread.start();
    }

    public void removeItemListener(ItemListener<E> listener) {
        ListenerThread thread = listenerMap.remove(listener);
        if (thread != null)
            thread.interrupt();
    }
}
