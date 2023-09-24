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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertConverter;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.TableField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

class Projector {
    private final List<TableField> fields;
    private final List<Expression<?>> projection;
    private final ExpressionEvalContext evalContext;

    private final UpsertConverter converter;

    Projector(
            List<TableField> fields,
            UpsertTarget target,
            List<Expression<?>> projection,
            ExpressionEvalContext evalContext
    ) {
        checkTrue(fields.size() == projection.size(), "fields.length != projection.length");
        this.fields = fields;
        this.projection = projection;
        this.evalContext = evalContext;

        converter = target.createConverter(fields);
    }

    Object project(JetSqlRow jetSqlRow) {
        Row row = jetSqlRow.getRow();
        List<Object> values = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Object projected = evaluate(projection.get(i), row, evalContext);
            values.add(getToConverter(fields.get(i).getType()).convert(projected));
        }
        return converter.apply(new RowValue(values));
    }

    static Supplier supplier(
            List<TableField> fields,
            UpsertTargetDescriptor descriptor,
            List<Expression<?>> projection
    ) {
        return new Supplier(fields, descriptor, projection);
    }

    static final class Supplier implements DataSerializable {
        private List<TableField> fields;
        private UpsertTargetDescriptor descriptor;
        private List<Expression<?>> projection;

        @SuppressWarnings("unused")
        private Supplier() { }

        private Supplier(
                List<TableField> fields,
                UpsertTargetDescriptor descriptor,
                List<Expression<?>> projection
        ) {
            this.fields = fields;
            this.descriptor = descriptor;
            this.projection = projection;
        }

        Projector get(ExpressionEvalContext evalContext) {
            return new Projector(
                    fields,
                    descriptor.create(evalContext.getSerializationService()),
                    projection,
                    evalContext
            );
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeList(fields, out);
            out.writeObject(descriptor);
            out.writeObject(projection);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            fields = SerializationUtil.readList(in);
            descriptor = in.readObject();
            projection = in.readObject();
        }
    }
}
