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

package com.hazelcast.multimap.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapPartitionContainer;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @ali 1/1/13
 */
public class ObjectMultiMapProxy<K, V> extends MultiMapProxySupport implements MultiMapProxy<K, V> {

    public ObjectMultiMapProxy(String name, MultiMapService service, NodeEngine nodeEngine) {
        super(name, service, nodeEngine);
    }

    public String getName() {

        return name;
    }

    public boolean put(K key, V value) {
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        return putInternal(dataKey,dataValue);
    }

    public Collection<V> get(K key) {
        Data dataKey = nodeEngine.toData(key);
        Collection<Data> dataColl = getInternal(dataKey);
        if (dataColl == null){
            return new HashSet<V>(0);
        }
        Collection<V> coll = new HashSet<V>(dataColl.size());
        for (Data data: dataColl){
            coll.add((V)IOUtil.toObject(data));
        }
        return coll;
    }

    public boolean remove(Object key, Object value) {
        return false;
    }

    public Collection<V> remove(Object key) {
        return null;
    }

    public Set<K> localKeySet() {
        return null;
    }

    public Set<K> keySet() {
        return null;
    }

    public Collection<V> values() {
        return null;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return null;
    }

    public boolean containsKey(K key) {
        return false;
    }

    public boolean containsValue(Object value) {
        return false;
    }

    public boolean containsEntry(K key, V value) {
        return false;
    }

    public int size() {
        return 0;
    }

    public void clear() {

    }

    public int valueCount(K key) {
        return 0;
    }

    public void addLocalEntryListener(EntryListener<K, V> listener) {

    }

    public void addEntryListener(EntryListener<K, V> listener, boolean includeValue) {

    }

    public void removeEntryListener(EntryListener<K, V> listener) {

    }

    public void addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {

    }

    public void removeEntryListener(EntryListener<K, V> listener, K key) {

    }

    public void lock(K key) {

    }

    public boolean tryLock(K key) {
        return false;
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) {
        return false;
    }

    public void unlock(K key) {

    }

    public LocalMapStats getLocalMultiMapStats() {
        int count = nodeEngine.getPartitionCount();
        for (int i=0; i<count; i++){
            MultiMapPartitionContainer partitionContainer = service.getPartitionContainer(i);
            Map<String, MultiMapContainer> multiMaps = partitionContainer.getMultiMaps();
            if (multiMaps.size() > 0){
                System.out.println("partitionId: " + i);
            }
            for (Map.Entry<String, MultiMapContainer> entry: multiMaps.entrySet()){
                System.out.println("\tname: " + entry.getKey());
                MultiMapContainer container = entry.getValue();
                Map<Data, Object> map = container.getObjects();
                for (Map.Entry<Data, Object> en: map.entrySet()){
                    System.out.println("\t\tkey: " + IOUtil.toObject(en.getKey()));
                    Collection col = (Collection)en.getValue();
                    for (Object o: col){
                        System.out.println("\t\t\tval: " + IOUtil.toObject(o));
                    }
                }
            }

        }
        return null;
    }

    public InstanceType getInstanceType() {
        return null;
    }

    public void destroy() {

    }

    public Object getId() {
        return null;
    }
}
