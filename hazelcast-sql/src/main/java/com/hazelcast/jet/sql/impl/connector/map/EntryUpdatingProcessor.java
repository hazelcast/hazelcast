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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static java.util.stream.Collectors.toList;

public final class EntryUpdatingProcessor
        implements EntryProcessor<Object, Object, Long>, SerializationServiceAware, DataSerializable {

    private KvRowProjector.Supplier rowProjector;
    private Projector.Supplier valueProjector;
    private List<Object> arguments;

    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private EntryUpdatingProcessor() {
    }

    private EntryUpdatingProcessor(
            KvRowProjector.Supplier rowProjector,
            Projector.Supplier valueProjector,
            List<Object> arguments
    ) {
        this.rowProjector = rowProjector;
        this.valueProjector = valueProjector;
        this.arguments = arguments;
    }

    @Override
    public Long process(Map.Entry<Object, Object> entry) {
        Object[] row = rowProjector.get(evalContext, extractors).project(entry.getKey(), entry.getValue());
        Object value = valueProjector.get(evalContext).project(row);
        if (value == null) {
            throw QueryException.error("Cannot assign null to value");
        } else {
            entry.setValue(value);
            return 1L;
        }
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.evalContext = new SimpleExpressionEvalContext(arguments, (InternalSerializationService) serializationService);
        this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(rowProjector);
        out.writeObject(valueProjector);
        out.writeObject(arguments);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        rowProjector = in.readObject();
        valueProjector = in.readObject();
        arguments = in.readObject();
    }

    public static Supplier supplier(
            PartitionedMapTable table,
            Map<String, Expression<?>> updatesByFieldNames
    ) {
        table.keyFields().filter(field -> updatesByFieldNames.containsKey(field.getName())).findFirst().ifPresent(field -> {
            throw QueryException.error("Cannot update '" + field.getName() + '\'');
        });
        if (updatesByFieldNames.containsKey(VALUE) && table.valueFields().count() > 1) {
            throw QueryException.error("Cannot update '" + VALUE + '\'');
        }

        List<Expression<?>> projections = IntStream.range(0, table.getFieldCount())
                .mapToObj(i -> ColumnExpression.create(i, table.getField(i).getType()))
                .collect(toList());
        KvRowProjector.Supplier rowProjectorSupplier = KvRowProjector.supplier(
                table.paths(),
                table.types(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                null,
                projections
        );

        List<Expression<?>> updates = IntStream.range(0, table.getFieldCount())
                .filter(i -> !((MapTableField) table.getField(i)).getPath().isKey())
                .mapToObj(i -> {
                    TableField field = table.getField(i);
                    if (updatesByFieldNames.containsKey(field.getName())) {
                        return updatesByFieldNames.get(field.getName());
                    } else if (field.getName().equals(VALUE)) {
                        // this works because assigning `this = null` is ignored if this is expanded to fields
                        return ConstantExpression.create(null, field.getType());
                    } else {
                        return ColumnExpression.create(i, field.getType());
                    }
                }).collect(toList());
        Projector.Supplier valueProjectorSupplier = Projector.supplier(
                table.valuePaths(),
                table.valueTypes(),
                (UpsertTargetDescriptor) table.getValueJetMetadata(),
                updates
        );

        return new Supplier(rowProjectorSupplier, valueProjectorSupplier);
    }

    public static final class Supplier implements DataSerializable {

        private KvRowProjector.Supplier rowProjectorSupplier;
        private Projector.Supplier valueProjectorSupplier;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                KvRowProjector.Supplier rowProjectorSupplier,
                Projector.Supplier valueProjectorSupplier
        ) {
            this.rowProjectorSupplier = rowProjectorSupplier;
            this.valueProjectorSupplier = valueProjectorSupplier;
        }

        public EntryProcessor<Object, Object, Long> get(List<Object> arguments) {
            return new EntryUpdatingProcessor(rowProjectorSupplier, valueProjectorSupplier, arguments);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rowProjectorSupplier);
            out.writeObject(valueProjectorSupplier);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rowProjectorSupplier = in.readObject();
            valueProjectorSupplier = in.readObject();
        }
    }
}
