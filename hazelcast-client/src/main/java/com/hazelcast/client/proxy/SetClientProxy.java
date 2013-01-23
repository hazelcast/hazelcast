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
import com.hazelcast.client.proxy.ProxyHelper;
import com.hazelcast.core.ISet;
//import com.hazelcast.impl.ClusterOperation;

import java.util.Collection;

import static com.hazelcast.client.proxy.ProxyHelper.check;

public class SetClientProxy<E> extends CollectionClientProxy<E> implements ISet<E> {

    public SetClientProxy(HazelcastClient client, String name) {
        super(client, name);
    }

    public SetClientProxy(ProxyHelper proxyHelper, String name) {
        super(proxyHelper, name);
    }

    @Override
    public boolean add(E o) {
        check(o);
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_ADD_TO_SET, o, null);
        return false;
    }

    @Override
    public boolean remove(Object o) {
        check(o);
        return false;
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_ITEM, o, null);
    }

    @Override
    public boolean contains(Object o) {
        check(o);
        return false;
//        return (Boolean) proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_KEY, o, null);
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
        return proxyHelper.keys(null);
    }

    public String getName() {
        return name;
    }

}
