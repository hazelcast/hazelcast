/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Instance;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class LightEntrySet<K, V> extends AbstractCollection<java.util.Map.Entry<K, V>> implements Set<java.util.Map.Entry<K, V>> {

    private final Set<K> keySet;
    private final MapClientProxy<K, V> proxy;
    private final Instance.InstanceType instanceType;

    public LightEntrySet(Set<K> set, MapClientProxy<K, V> proxy, Instance.InstanceType instanceType) {
        this.keySet = set;
        this.proxy = proxy;
        this.instanceType = instanceType;
    }

    public Iterator<Entry<K, V>> iterator() {
        return new MapEntryIterator<K, V>(keySet.iterator(), proxy, instanceType);
    }

    public int size() {
        return keySet.size();
    }
}
