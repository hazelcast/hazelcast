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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.extract.AvroQueryTarget;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.EvictableGetterCache;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.GetterCache;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * An expression backing the DOT operator for extracting field from a struct type.
 * <p>
 * {@code ref.field} extracts {@code field} from {@code ref}.
 */
public class FieldAccessExpression<T> implements Expression<T> {
    // FAE can be potentially used for many subclasses of the base class, but it will always use same getter.
    private static final int MAX_CLASS_COUNT = 10;
    private static final int MAX_GETTER_PER_CLASS_COUNT = 1;

    // Single instance for all calls to eval, used only during execution on particular node.
    private transient volatile GetterCache getterCache;

    private QueryDataType type;
    private String name;
    private Expression<?> ref;

    public FieldAccessExpression() { }

    private FieldAccessExpression(
            final QueryDataType type,
            final String name,
            final Expression<?> ref
    ) {
        this.type = type;
        this.name = name;
        this.ref = ref;
    }

    public static FieldAccessExpression<?> create(
            final QueryDataType type,
            final String name,
            final Expression<?> ref
    ) {
        return new FieldAccessExpression<>(type, name, ref);
    }


    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        return eval(row, context, false);
    }

    @Override
    public T eval(final Row row, final ExpressionEvalContext context, boolean useLazyDeserialization) {
        // Use lazy deserialization for nested queries. Only the last access should be eager.
        final Object result = ref.eval(row, context, true);
        if (result == null) {
            return null;
        }

        if (isPrimitive(result.getClass())) {
            throw QueryException.error("Field Access expression can not be applied to primitive types");
        }

        if (getterCache == null) {
            getterCache = new EvictableGetterCache(
                    MAX_CLASS_COUNT,
                    MAX_GETTER_PER_CLASS_COUNT,
                    GetterCache.EVICTABLE_CACHE_EVICTION_PERCENTAGE,
                    false
            );
        }

        try {
            Object value = result instanceof GenericRecord
                    ? AvroQueryTarget.extractValue((GenericRecord) result, name)
                    : Extractors.newBuilder(context.getSerializationService())
                            .setGetterCacheSupplier(() -> getterCache)
                            .build().extract(result, name, useLazyDeserialization);
            return (T) type.convert(value);
        } catch (Exception e) {
            throw QueryException.error("Failed to extract field");
        }
    }

    private boolean isPrimitive(Class<?> clazz) {
        return clazz.getPackage().getName().startsWith("java.");
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_FIELD_ACCESS;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeString(name);
        out.writeObject(ref);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        type = in.readObject();
        name = in.readString();
        ref = in.readObject();
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public boolean isCooperative() {
        return ref.isCooperative();
    }
}
