/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertConverter;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;
import static java.util.stream.Collectors.toList;

/**
 * A utility to convert a row represented as {@link JetSqlRow} to a
 * key-value entry represented as {@code Entry<Object, Object>}.
 * <p>
 * {@link KvRowProjector} does the reverse.
 */
public class KvProjector {
    private final List<TableField> fields;
    private final boolean failOnNulls;

    private final UpsertConverter keyConverter;
    private final UpsertConverter valueConverter;

    KvProjector(
            List<TableField> fields,
            UpsertTarget keyTarget,
            UpsertTarget valueTarget,
            boolean failOnNulls
    ) {
        this.fields = fields;
        this.failOnNulls = failOnNulls;

        keyConverter = keyTarget.createConverter(fields.stream()
                .filter(field -> ((MapTableField) field).getPath().isKey()).collect(toList()));
        valueConverter = valueTarget.createConverter(fields.stream()
                .filter(field -> !((MapTableField) field).getPath().isKey()).collect(toList()));
    }

    public Entry<Object, Object> project(JetSqlRow row) {
        List<Object> keyFields = new ArrayList<>(fields.size());
        List<Object> valueFields = new ArrayList<>(fields.size());
        for (int i = 0; i < row.getFieldCount(); i++) {
            MapTableField field = (MapTableField) fields.get(i);
            Object converted = getToConverter(field.getType()).convert(row.get(i));
            (field.getPath().isKey() ? keyFields : valueFields).add(converted);
        }

        Object key = keyConverter.apply(new RowValue(keyFields));
        if (key == null && failOnNulls) {
            throw QueryException.error("Cannot write NULL to '__key' field. " +
                    "Note that NULL is used also if your INSERT/SINK command doesn't write to '__key' at all.");
        }

        Object value = valueConverter.apply(new RowValue(valueFields));
        if (value == null && failOnNulls) {
            throw QueryException.error("Cannot write NULL to 'this' field. " +
                    "Note that NULL is used also if your INSERT/SINK command doesn't write to 'this' at all.");
        }

        return entry(key, value);
    }

    public static Supplier supplier(
            List<TableField> fields,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            boolean failOnNulls
    ) {
        return new Supplier(fields, keyDescriptor, valueDescriptor, failOnNulls);
    }

    public static final class Supplier implements DataSerializable {
        private List<TableField> fields;

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        private boolean failOnNulls;

        @SuppressWarnings("unused")
        private Supplier() { }

        private Supplier(
                List<TableField> fields,
                UpsertTargetDescriptor keyDescriptor,
                UpsertTargetDescriptor valueDescriptor,
                boolean failOnNulls
        ) {
            this.fields = fields;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
            this.failOnNulls = failOnNulls;
        }

        public KvProjector get(InternalSerializationService serializationService) {
            return new KvProjector(
                    fields,
                    keyDescriptor.create(serializationService),
                    valueDescriptor.create(serializationService),
                    failOnNulls
            );
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeList(fields, out);
            out.writeObject(keyDescriptor);
            out.writeObject(valueDescriptor);
            out.writeBoolean(failOnNulls);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            fields = SerializationUtil.readList(in);
            keyDescriptor = in.readObject();
            valueDescriptor = in.readObject();
            failOnNulls = in.readBoolean();
        }
    }
}
