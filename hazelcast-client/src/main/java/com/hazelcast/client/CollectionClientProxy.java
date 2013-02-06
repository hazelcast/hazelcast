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

package com.hazelcast.client;

import com.hazelcast.client.proxy.ProxyHelper;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

public abstract class CollectionClientProxy<E> extends AbstractCollection<E> {
    final protected ProxyHelper proxyHelper;
    final protected String name;
    final protected HazelcastClient client;

    public CollectionClientProxy(HazelcastClient hazelcastClient, String name) {
        this.name = name;
        this.client = hazelcastClient;
        proxyHelper = new ProxyHelper(hazelcastClient.getSerializationService(), hazelcastClient.getConnectionPool());
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

    @Override
    public int size() {
        return 0;
//        return (Integer) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
