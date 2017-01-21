/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.getters.Extractors;

import java.io.IOException;

/**
 * Entry of the Query.
 */
public class CachedQueryEntry extends QueryableEntry implements IdentifiedDataSerializable {

    protected Data keyData;
    protected Object keyObject;
    protected Data valueData;
    protected Object valueObject;

    public CachedQueryEntry() {
    }

    public CachedQueryEntry(InternalSerializationService serializationService, Data key, Object value, Extractors extractors) {
        init(serializationService, key, value, extractors);
    }

    public void init(InternalSerializationService serializationService, Data key, Object value, Extractors extractors) {
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }
        this.serializationService = serializationService;
        this.keyData = key;
        this.keyObject = null;

        if (value instanceof Data) {
            this.valueData = (Data) value;
            this.valueObject = null;
        } else {
            this.valueObject = value;
            this.valueData = null;
        }
        this.extractors = extractors;
    }

    @Override
    public Object getKey() {
        if (keyObject == null) {
            keyObject = serializationService.toObject(keyData);
        }
        return keyObject;
    }

    @Override
    public Object getValue() {
        if (valueObject == null) {
            valueObject = serializationService.toObject(valueData);
        }
        return valueObject;
    }

    @Override
    public Data getKeyData() {
        return keyData;
    }

    @Override
    public Data getValueData() {
        if (valueData == null) {
            valueData = serializationService.toData(valueObject);
        }
        return valueData;
    }

    @Override
    protected Object getTargetObject(boolean key) {
        Object targetObject;
        if (key) {
            //keyData is never null
            if (keyData.isPortable()) {
                targetObject = keyData;
            } else {
                targetObject = getKey();
            }
        } else {
            if (valueObject == null) {
                if (valueData.isPortable()) {
                    targetObject = valueData;
                } else {
                    targetObject = getValue();
                }
            } else {
                if (valueObject instanceof Portable) {
                    targetObject = getValueData();
                } else {
                    targetObject = getValue();
                }
            }
        }
        return targetObject;
    }

    @Override
    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CachedQueryEntry that = (CachedQueryEntry) o;
        if (!keyData.equals(that.keyData)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return keyData.hashCode();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.CACHED_QUERY_ENTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData(keyData);
        out.writeData(valueData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyData = in.readData();
        valueData = in.readData();
    }

    @Override
    public void inflate(InternalSerializationService serializationService) {
        // In case of migrations the CachedQueryEntry may be serialized.
        // In this case the serializationService and extractors will be null.
        // Extractors are not needed after deserialization since the filtering happened on the node where
        // the QueryEntry was populated. But the serializationService may still be needed
        // when e.g. valueObject is fetched by Projections.
        this.serializationService = serializationService;
    }

}
