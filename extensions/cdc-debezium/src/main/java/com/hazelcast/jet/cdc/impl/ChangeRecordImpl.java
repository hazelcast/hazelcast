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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.jet.cdc.RecordPart;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ChangeRecordImpl implements ChangeRecord, Serializable {
    private static final long serialVersionUID = 1L;

    private final long sequenceSource;
    private final long sequenceValue;
    private final String keyJson;

    private final Long timestamp;
    private final Operation operation;
    private final String database;
    private final String schema;
    private final String table;
    private RecordPart key;
    private final RecordPart oldValue;
    private final RecordPart newValue;

    public ChangeRecordImpl(
            long timestamp,
            long sequenceSource,
            long sequenceValue,
            Operation operation,
            @Nonnull String keyJson,
            @Nullable String oldValueJson,
            @Nullable String newValueJson,
            String table,
            String schema,
            String database
    ) {
        this.timestamp = timestamp;
        this.sequenceSource = sequenceSource;
        this.sequenceValue = sequenceValue;
        this.operation = operation;
        this.keyJson = requireNonNull(keyJson, "keyJson");
        this.oldValue = oldValueJson == null ? null : new RecordPartImpl(oldValueJson);
        this.newValue = newValueJson == null ? null : new RecordPartImpl(newValueJson);
        this.table = table;
        this.schema = schema;
        this.database = database;
    }

    @Override
    public long timestamp() throws ParsingException {
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        return operation;
    }

    @Nonnull
    @Override
    public String database() {
        return database;
    }

    @Nonnull
    @Override
    public String schema() {
        return schema;
    }

    @Nonnull
    @Override
    public String table() {
        return table;
    }

    @Override
    @Nonnull
    public RecordPart key() {
        if (key == null) {
            key = new RecordPartImpl(keyJson);
        }
        return key;
    }

    @Override
    @Nonnull
    public RecordPart value() {
        switch (operation) {
            case SYNC:
            case INSERT:
            case UPDATE: return newValue();
            case DELETE: return oldValue();
            default: throw new IllegalArgumentException("cannot call .value() for operation " + operation);
        }
    }
    @Override
    @Nonnull
    public RecordPart newValue() {
        return newValue;
    }
    @Override
    @Nonnull
    public RecordPart oldValue() {
        return oldValue;
    }
    @Override
    public String getNewValueJson() {
        return newValue == null ? null : newValue.toJson();
    }
    @Override
    public String getOldValueJson() {
        return oldValue == null ? null : oldValue.toJson();
    }

    @Override
    @Nonnull
    public String toJson() {
        return String.format("key:{%s}, value:{%s}", keyJson,
                operation == Operation.DELETE ? getOldValueJson() : getNewValueJson());
    }

    @Override
    public long sequenceSource() {
        return sequenceSource;
    }

    @Override
    public long sequenceValue() {
        return sequenceValue;
    }

    public String getKeyJson() {
        return keyJson;
    }

    public String getValueJson() {
        switch (operation) {
            case SYNC:
            case INSERT:
            case UPDATE: return getNewValueJson();
            case DELETE: return getOldValueJson();
            default: throw new IllegalArgumentException("cannot call .getValueJson() for operation " + operation);
        }
    }

    @Override
    public String toString() {
        return toJson();
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(Map<String, Object> map, String key, Class<T> clazz) {
        Object obj = map.get(key);
        if (clazz.isInstance(obj)) {
            return (T) obj;
        }
        return null;
    }

    @Override
    public int hashCode() {
        int hash = (int) sequenceSource;
        hash = 31 * hash + (int) sequenceValue;
        hash = 31 * hash + keyJson.hashCode();
        hash = 31 * hash + oldValue.toJson().hashCode();
        hash = 31 * hash + newValue.toJson().hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ChangeRecordImpl that = (ChangeRecordImpl) obj;
        return this.sequenceSource == that.sequenceSource &&
                this.sequenceValue == that.sequenceValue &&
                this.keyJson.equals(that.keyJson) &&
                this.oldValue.equals(that.oldValue) &&
                this.newValue.equals(that.newValue);
    }
}
