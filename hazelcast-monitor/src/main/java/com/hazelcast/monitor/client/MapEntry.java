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
package com.hazelcast.monitor.client;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class MapEntry implements Map.Entry, IsSerializable, Serializable {

    private long cost = 0;
    private Date expirationTime;
    private Date lastAccessTime;
    private Date lastUpdateTime;
    private Date creationTime;
    private long version = 0;
    private long hits = 0;
    private boolean valid = true;
    private String key = null;
    private String keyClass = null;
    private String value = null;
    private String valueClass = null;

    public String getKey() {
        return key;
    }

    public String getKeyClass() {
        return keyClass;
    }
    
    public String getValue() {
        return value;
    }
    
    public String getValueClass() {
        return valueClass;
    }
    
    public void setKey(Object key) {
        this.key = String.valueOf(key);
        this.keyClass = key.getClass().getName();
    }

    public Object setValue(Object value) {
        Object oldValue = this.value;
        this.value = String.valueOf(value);
        this.valueClass = value.getClass().getName();
        return oldValue;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public Date getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = new Date(expirationTime);
    }

    public Date getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = new Date(lastAccessTime);
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = new Date(lastUpdateTime);
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = new Date(creationTime);
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
}
