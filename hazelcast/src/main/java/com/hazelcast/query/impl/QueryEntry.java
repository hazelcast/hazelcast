/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Entry of the Query.
 */
public class QueryEntry extends QueryableEntry {

    private Data key;
    private Object value;

    public QueryEntry() {
    }

    public QueryEntry(SerializationService serializationService, Data key, Object value, Extractors extractors) {
        init(serializationService, key, value, extractors);
    }

    /**
     * It may be useful to use this {@code init} method in some cases that same instance of this class can be used
     * instead of creating a new one for every iteration when scanning large data sets, for example:
     * <pre>
     * <code>Predicate predicate = ...
     * QueryEntry entry = new QueryEntry()
     * for (i == 0; i &lt; HUGE_NUMBER; i++) {
     *       entry.init(...)
     *       boolean valid = predicate.apply(queryEntry);
     *
     *       if (valid) {
     *          ....
     *       }
     *  }
     * </code>
     * </pre>
     */
    public void init(SerializationService serializationService, Data key, Object value, Extractors extractors) {
        if (key == null) {
            throw new IllegalArgumentException("keyData cannot be null");
        }

        this.serializationService = (InternalSerializationService) serializationService;

        this.key = key;
        this.value = value;
        this.extractors = extractors;
    }

    @Override
    public Object getKey() {
        return serializationService.toObject(key);
    }

    @Override
    public Data getKeyData() {
        return key;
    }

    @Override
    public Object getValue() {
        return serializationService.toObject(value);
    }

    @Override
    public Data getValueData() {
        return serializationService.toData(value);
    }

    @Override
    public Object getKeyIfPresent() {
        return null;
    }

    @Override
    public Data getKeyDataIfPresent() {
        return key;
    }

    @Override
    public Object getValueIfPresent() {
        if (!(value instanceof Data)) {
            return value;
        }

        Object possiblyNotData = record.getValue();

        return possiblyNotData instanceof Data ? null : possiblyNotData;
    }

    @Override
    public Data getValueDataIfPresent() {
        if (value instanceof Data) {
            return (Data) value;
        }

        Object possiblyData = record.getValue();

        return possiblyData instanceof Data ? (Data) possiblyData : null;
    }

    @Override
    protected Object getTargetObject(boolean key) {
        return key ? this.key : this.value;
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
        QueryEntry that = (QueryEntry) o;
        if (!key.equals(that.key)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
