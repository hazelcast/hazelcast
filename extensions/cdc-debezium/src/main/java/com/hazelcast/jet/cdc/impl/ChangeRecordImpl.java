/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;

public class ChangeRecordImpl implements ChangeRecord {

    private final long sequenceSource;
    private final long sequenceValue;
    private final String keyJson;

    private final Long timestamp;
    private final Operation operation;
    private final String database;
    private final String schema;
    private final String table;
    private final RecordPartImpl source;
    private RecordPart key;
    private final RecordPart oldValue;
    private final RecordPart newValue;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public ChangeRecordImpl(
            long timestamp,
            long sequenceSource,
            long sequenceValue,
            Operation operation,
            @Nullable String keyJson,
            Supplier<String> sourceSupplier,
            Supplier<String> oldValueJsonSupplier,
            Supplier<String> newValueJsonSupplier,
            String table,
            String schema,
            String database
    ) {
        this.timestamp = timestamp;
        this.sequenceSource = sequenceSource;
        this.sequenceValue = sequenceValue;
        this.operation = operation;
        this.keyJson = keyJson;
        this.source = new RecordPartImpl(sourceSupplier);
        this.oldValue = oldValueJsonSupplier == null ? null : new RecordPartImpl(oldValueJsonSupplier);
        this.newValue = newValueJsonSupplier == null ? null : new RecordPartImpl(newValueJsonSupplier);
        this.table = table;
        this.schema = schema;
        this.database = database;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    ChangeRecordImpl(
            long timestamp,
            long sequenceSource,
            long sequenceValue,
            Operation operation,
            String keyJson,
            String sourceJson,
            String oldValueJson,
            String newValueJson,
            String table,
            String schema,
            String database
    ) {
        this.timestamp = timestamp;
        this.sequenceSource = sequenceSource;
        this.sequenceValue = sequenceValue;
        this.operation = operation;
        this.keyJson = keyJson;
        this.source = oldValueJson == null ? null : new RecordPartImpl(sourceJson);
        this.oldValue = oldValueJson == null ? null : new RecordPartImpl(oldValueJson);
        this.newValue = newValueJson == null ? null : new RecordPartImpl(newValueJson);
        this.table = table;
        this.schema = schema;
        this.database = database;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() {
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

    @Nonnull
    @Override
    public RecordPart source() {
        return source;
    }

    @Override
    @Nullable
    public RecordPart key() {
        if (key == null) {
            if (keyJson == null) {
                return null;
            }
            key = new RecordPartImpl(() -> keyJson);
        }
        return key;
    }

    @Override
    @Nullable
    public RecordPart value() {
        return switch (operation) {
            case UNSPECIFIED, SYNC, INSERT, UPDATE -> newValue();
            case DELETE -> oldValue();
        };
    }

    @Override
    @Nonnull
    public RecordPart nonNullValue() {
        return Objects.requireNonNull(value(), "value should not be null");
    }

    @Override
    @Nullable
    public RecordPart newValue() {
        return newValue;
    }
    @Override
    @Nullable
    public RecordPart oldValue() {
        return oldValue;
    }

    @Override
    @Nonnull
    public String toJson() {
        return String.format("key:{%s}, value:{%s}", keyJson, nonNullValue().toJson());
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

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceSource, sequenceValue, keyJson, timestamp,
                operation, database, schema, table, oldValue, newValue);
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ChangeRecordImpl that = (ChangeRecordImpl) obj;
        return this.sequenceSource == that.sequenceSource
                && this.sequenceValue == that.sequenceValue
                && Objects.equals(this.keyJson, that.keyJson)
                && Objects.equals(this.timestamp, that.timestamp)
                && this.operation == that.operation
                && Objects.equals(this.database, that.database)
                && Objects.equals(this.schema, that.schema)
                && Objects.equals(this.table, that.table)
                && Objects.equals(this.oldValue, that.oldValue)
                && Objects.equals(this.newValue, that.newValue);
    }
}
