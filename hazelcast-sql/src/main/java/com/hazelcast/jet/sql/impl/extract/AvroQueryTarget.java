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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class AvroQueryTarget implements QueryTarget {

    private GenericRecord record;

    @Override
    public void setTarget(Object target, Data targetData) {
        assert targetData == null;
        record = (GenericRecord) target;
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return path == null ? createExtractor() : createFieldExtractor(path, type);
    }

    private QueryExtractor createExtractor() {
        return () -> record;
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        return () -> type.convert(extractValue(record, path));
    }

    private static Object extractValue(GenericRecord record, String path) {
        if (!record.hasField(path)) {
            return null;
        }
        Object value = record.get(path);
        if (value instanceof Utf8) {
            return ((Utf8) value).toString();
        } else {
            return value;
        }
    }
}
