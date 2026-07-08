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
import com.hazelcast.jet.cdc.RecordMappingFunction;
import com.hazelcast.jet.cdc.SequenceExtractor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.Properties;
import java.util.function.Supplier;

import static com.hazelcast.jet.cdc.impl.ReadCdcP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY;
import static com.hazelcast.jet.impl.util.JetExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;

public class ChangeRecordMappingFn implements RecordMappingFunction<ChangeRecord> {

    @Serial
    private static final long serialVersionUID = 1L;

    private SequenceExtractor sequenceExtractor;

    @Override
    public void init(@Nonnull Properties properties, ClassLoader classLoader) {
        try {
            sequenceExtractor = newInstance(classLoader, properties.getProperty(SEQUENCE_EXTRACTOR_CLASS_PROPERTY),
                    "sequence extractor ");
        } catch (Exception e) {
            throw rethrow(e);
        }
    }


    @Override
    public ChangeRecord applyEx(SourceRecord record) {
        if (record == null || record.topic().startsWith("__debezium")) {
            // internal Debezium messages about e.g. Heartbeat uses such topics
            return null;
        }

        long sequenceSource = sequenceExtractor.source(record.sourcePartition(), record.sourceOffset());
        long sequenceValue = sequenceExtractor.sequence(record.sourceOffset());
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        Struct source = (Struct) value.get("source");
        Schema sourceSchema = valueSchema.field("source").schema();
        Supplier<String> sourceJson = () -> Values.convertToString(sourceSchema, source);

        Operation operation = value.schema().field("op") != null
                ? Operation.get(value.getString("op"))
                : Operation.UNSPECIFIED;

        long timestamp;
        Supplier<String> oldValueJson;
        Supplier<String> newValueJson;
        if (operation == Operation.UNSPECIFIED) {
            timestamp = ((Struct) value.get("source")).getInt64("ts_ms");
            oldValueJson = () -> Values.convertToString(valueSchema, value);
            newValueJson = () -> Values.convertToString(valueSchema, value);
        } else {
            Object before = valueSchema.field("before") != null ? value.get("before") : null;
            Object after = valueSchema.field("after") != null ? value.get("after") : null;

            timestamp = value.getInt64("ts_ms");
            oldValueJson = before == null
                    ? null
                    : () -> StructToMap.toJson(before);
            newValueJson = after == null
                    ? null
                    : () -> StructToMap.toJson(after);
        }

        return new ChangeRecordImpl(
                timestamp,
                sequenceSource,
                sequenceValue,
                operation,
                keyJson,
                sourceJson,
                oldValueJson,
                newValueJson,
                fieldOrNull(source, "table"),
                fieldOrNull(source, "schema"),
                fieldOrNull(source, "db")
        );
    }

    private static String fieldOrNull(Struct struct, String fieldName) {
        return struct.schema().field(fieldName) != null
                ? struct.getString(fieldName)
                : null;
    }
}
