/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class AvroUpsertTarget implements UpsertTarget {

    private final Schema schema;

    private GenericRecordBuilder record;

    AvroUpsertTarget(String schema) {
        this.schema = new Schema.Parser().parse(schema);
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        switch (type.getTypeFamily()) {
            case TINYINT:
                return value -> record.set(path, value == null ? null : ((Byte) value).intValue());
            case SMALLINT:
                return value -> record.set(path, value == null ? null : ((Short) value).intValue());
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case REAL:
            case DOUBLE:
                return value -> record.set(path, value);
            case DECIMAL:
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case VARCHAR:
                return value -> record.set(path, QueryDataType.VARCHAR.convert(value));
            case OBJECT:
                return createObjectInjector(path);
            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    private UpsertInjector createObjectInjector(String path) {
        return value -> {
            if (value == null) {
                record.set(path, null);
            } else if (value instanceof GenericContainer) {
                throw QueryException.error("Cannot set value of type " + value.getClass().getName()
                        + " to field \"" + path + "\"");
            } else if (value instanceof Byte) {
                record.set(path, ((Byte) value).intValue());
            } else if (value instanceof Short) {
                record.set(path, ((Short) value).intValue());
            } else if (value instanceof Boolean
                    || value instanceof Integer
                    || value instanceof Long
                    || value instanceof Float
                    || value instanceof Double
            ) {
                record.set(path, value);
            } else {
                record.set(path, QueryDataType.VARCHAR.convert(value));
            }
        };
    }

    @Override
    public void init() {
        record = new GenericRecordBuilder(schema);
    }

    @Override
    public Object conclude() {
        Record record = this.record.build();
        this.record = null;
        return record;
    }
}
