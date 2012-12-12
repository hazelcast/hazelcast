/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.impl.ItemListenerManager;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.client.ProxyHelper.check;

public abstract class CollectionClientProxy<E> extends AbstractCollection<E> {
    final protected ProxyHelper proxyHelper;
    final protected String name;

    public CollectionClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        proxyHelper = new ProxyHelper(name, hazelcastClient);
    }

    public CollectionClientProxy(ProxyHelper proxyHelper, String name) {
        this.name = name;
        this.proxyHelper = proxyHelper;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }

    @Override
    public Iterator<E> iterator() {
        final Collection<E> collection = getTheCollection();
        final AbstractCollection<E> proxy = this;
        return new Iterator<E>() {
            Iterator<E> iterator = collection.iterator();
            volatile E lastRecord;

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public E next() {
                lastRecord = iterator.next();
                return lastRecord;
            }

            public void remove() {
                iterator.remove();
                proxy.remove(lastRecord);
            }
        };
    }

    abstract protected Collection<E> getTheCollection();

    @Override
    public String toString() {
        return "CollectionClientProxy{" +
                "name='" + name + '\'' +
                '}';
    }

    public synchronized void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
        Call c = itemListenerManager().createNewAddListenerCall(proxyHelper, includeValue);
        itemListenerManager().registerListener(name, listener, includeValue);
        proxyHelper.doCall(c);
    }

    public synchronized void removeItemListener(ItemListener<E> listener) {
        check(listener);
        itemListenerManager().removeListener(name, listener);
        Packet request = proxyHelper.createRequestPacket(ClusterOperation.REMOVE_LISTENER, null, null);
        Call c = proxyHelper.createCall(request);
        proxyHelper.doCall(c);
    }

    private ItemListenerManager itemListenerManager() {
        return proxyHelper.getHazelcastClient().getListenerManager().getItemListenerManager();
    }

    @Override
    public int size() {
        return (Integer) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
