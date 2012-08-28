/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;

public class ClientMapEntry<K, V> implements MapEntry<K, V> {
    private final MapClientProxy<K,V> proxy;

    private final K key;
    private V value;
    private long cost = 0;
    private long expirationTime = 0;
    private long lastAccessTime = 0;
    private long lastUpdateTime = 0;
    private long lastStoredTime = 0;
    private long creationTime = 0;
    private long version = 0;
    private int hits = 0;
    private boolean valid = true;
    private String name = null;

    public ClientMapEntry(K key, MapClientProxy<K,V> proxy) {
        this.key = key;
        this.proxy = proxy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public V setValue(V value) {
        return proxy.put(key, value);
    }
    public void setV(V value) {
        this.value = value;
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
