/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.client;

import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Instance;
import com.hazelcast.impl.ClusterOperation;

import java.util.Collection;
import java.util.Set;

public class MultiMapClientProxy implements ClientProxy, MultiMap{
    private final HazelcastClient client;
    private final String name;
    private final ProxyHelper proxyHelper;

    public MultiMapClientProxy(HazelcastClient client, String name){
        this.name = name;
        this.client = client;
        proxyHelper = new ProxyHelper(name, client);
    }

    public void setOutRunnable(OutRunnable out) {
        proxyHelper.setOutRunnable(out);
    }

    public String getName() {
        return name.substring(4);
    }

    public boolean put(Object key, Object value) {
        return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_PUT_MULTI, key, value);
    }

    public Collection get(Object key) {
        return (Collection)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_GET, key, null);
    }

    public boolean remove(Object key, Object value) {
        return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_MULTI, key, value);
    }

    public Collection remove(Object key) {
//        return (Collection)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_REMOVE_MULTI, key, null);
        return null;
    }

    public Set keySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Collection values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set entrySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean containsKey(Object key) {
        return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS, key, null);
    }

    public boolean containsValue(Object value) {
        return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, null, value);
    }

    public boolean containsEntry(Object key, Object value) {
        return (Boolean)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_CONTAINS_VALUE, key, value);
    }

    public int size() {
        return (Integer)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
    }

    public void clear() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public int valueCount(Object key) {
        return 0;
    }

    public InstanceType getInstanceType() {
        return InstanceType.MULTIMAP;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return name;
    }
}
