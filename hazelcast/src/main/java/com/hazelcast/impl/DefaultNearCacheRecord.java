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

package com.hazelcast.impl;

import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class DefaultNearCacheRecord implements NearCacheRecord {

    private final Data keyData;
    private Data valueData = null;
    private Object value = null;

    public DefaultNearCacheRecord(Data keyData, Data valueData) {
        super();
        this.keyData = keyData;
        this.valueData = valueData;
    }

    public void setValueData(Data valueData) {
        this.valueData = valueData;
        this.value = null;
    }

    public Data getValueData() {
        return valueData;
    }

    public Data getKeyData() {
        return keyData;
    }

    public boolean hasValueData() {
        return valueData != null;
    }

    public Object getValue() {
        if (value != null) {
            return value;
        }
        if (!hasValueData()) {
            return null;
        }
        value = toObject(valueData);
        return value;
    }

    public void invalidate() {
        setValueData(null);
    }
}
