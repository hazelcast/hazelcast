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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.proxy.listener.ItemEventLRH;
import com.hazelcast.client.proxy.listener.ListenerThread;
import com.hazelcast.collection.set.ObjectSetProxy;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import com.hazelcast.deprecated.nio.Protocol;
import com.hazelcast.deprecated.nio.protocol.Command;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.client.proxy.ProxyHelper.check;
//import com.hazelcast.impl.ClusterOperation;

public class SetClientProxy<E> extends CollectionClientProxy<E> implements ISet<E> {
    private Map<ItemListener, ListenerThread> listenerMap = new ConcurrentHashMap<ItemListener, ListenerThread>();

    public SetClientProxy(HazelcastClient client, String name) {
        super(client, name);
    }

    @Override
    public boolean add(E o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.SADD, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public boolean remove(Object o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.SREMOVE, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public int size() {
        return proxyHelper.doCommandAsInt(Command.SSIZE, new String[]{getName()});
    }

    @Override
    public boolean contains(Object o) {
        check(o);
        return proxyHelper.doCommandAsBoolean(Command.SCONTAINS, new String[]{getName()}, proxyHelper.toData(o));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ISet && o != null) {
            return getName().equals(((ISet) o).getName());
        } else {
            return false;
        }
    }

    @Override
    protected Collection<E> getTheCollection() {
        return proxyHelper.doCommandAsList(Command.SGETALL, new String[]{getName()});
    }

    public String getName() {
        return name;
    }

    public void destroy() {
        proxyHelper.doCommand(Command.DESTROY, new String[]{ObjectSetProxy.COLLECTION_SET_NAME, getName()});
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        Protocol request = proxyHelper.createProtocol(Command.SLISTEN, new String[]{getName(), String.valueOf(includeValue)}, null);
        ListenerThread thread = proxyHelper.createAListenerThread("hz.client.setListener.",
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
