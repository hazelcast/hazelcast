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

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.CMap.CMapEntry;

public class ClientMapEntry<K, V> implements MapEntry<K, V> {
    private final CMapEntry mapEntry;
    private final K key;
    private final MapClientProxy<K, V> proxy;

    public ClientMapEntry(CMapEntry mapEntry, K key, MapClientProxy<K, V> proxy) {
        this.mapEntry = mapEntry;
        this.key = key;
        this.proxy = proxy;
    }

    public long getCost() {
        return mapEntry.getCost();
    }

    public long getCreationTime() {
        return mapEntry.getCreationTime();
    }

    public long getExpirationTime() {
        return mapEntry.getExpirationTime();
    }

    public int getHits() {
        return mapEntry.getHits();
    }

    public long getLastAccessTime() {
        return mapEntry.getLastAccessTime();
    }

    public long getLastStoredTime() {
        return mapEntry.getLastStoredTime();
    }

    public long getLastUpdateTime() {
        return mapEntry.getLastUpdateTime();
    }

    public long getVersion() {
        return mapEntry.getVersion();
    }

    public boolean isValid() {
        return mapEntry.isValid();
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return proxy.get(key);
    }

    public V setValue(V value) {
        return proxy.put(key, value);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("MapEntry");
        sb.append("{key=").append(key);
        sb.append(", valid=").append(isValid());
        sb.append(", hits=").append(getHits());
        sb.append(", version=").append(getVersion());
        sb.append(", creationTime=").append(getCreationTime());
        sb.append(", lastUpdateTime=").append(getLastUpdateTime());
        sb.append(", lastAccessTime=").append(getLastAccessTime());
        sb.append(", expirationTime=").append(getExpirationTime());
        sb.append(", cost=").append(getCost());
        sb.append('}');
        return sb.toString();
    }
}
