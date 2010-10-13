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

package com.hazelcast.impl;

import com.hazelcast.core.MapEntry;

import static com.hazelcast.nio.IOUtil.toObject;

public class RecordEntry implements MapEntry {

    final Record record;
    private volatile Object keyObject;
    private volatile Object valueObject;

    public RecordEntry(Record record) {
        this.record = record;
    }

    public boolean isValid() {
        return record.isActive();
    }

    public Object getKey() {
        if (keyObject == null) {
            keyObject = toObject(record.getKey());
        }
        return keyObject;
    }

    public Object getValue() {
        if (valueObject == null) {
            valueObject = toObject(record.getValue());
        }
        return valueObject;
    }

    public Object setValue(Object value) {
        MProxy proxy = (MProxy) record.getCMap().concurrentMapManager.node.factory.getOrCreateProxyByName(record.getName());
        Object oldValue = proxy.put(getKey(), value);
        this.valueObject = value;
        return oldValue;
    }

    public long getCost() {
        return record.getCost();
    }

    public long getExpirationTime() {
        return record.getExpirationTime();
    }

    public long getVersion() {
        return record.getVersion();
    }

    public long getCreationTime() {
        return record.getCreationTime();
    }

    public long getLastAccessTime() {
        return record.getLastAccessTime();
    }

    public long getLastUpdateTime() {
        return record.getLastUpdateTime();
    }

    public int getHits() {
        return record.getHits();
    }
}
