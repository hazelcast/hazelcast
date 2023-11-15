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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.expression.json.JsonCreationUtil;
import com.hazelcast.query.impl.getters.EvictableGetterCache;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.getters.GetterCache;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.newSetFromMap;

public class UdtObjectToJsonFunction extends UniExpressionWithType<HazelcastJsonValue> {
    // Unlike FieldAccessExpression UdtToJson function typically works with many classes and all of their fields
    private static final int MAX_CLASS_COUNT = 64;
    private static final int MAX_GETTER_PER_CLASS_COUNT = 100;

    // single instance for all calls to eval, used only during execution on particular node
    private final GetterCache getterCache = new EvictableGetterCache(
            MAX_CLASS_COUNT,
            MAX_GETTER_PER_CLASS_COUNT,
            GetterCache.EVICTABLE_CACHE_EVICTION_PERCENTAGE,
            false
    );

    public UdtObjectToJsonFunction() { }

    private UdtObjectToJsonFunction(Expression<?> operand) {
        super(operand, QueryDataType.JSON);
    }

    public static UdtObjectToJsonFunction create(Expression<?> operand) {
        return new UdtObjectToJsonFunction(operand);
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final Object obj = this.operand.eval(row, context);
        final QueryDataType queryDataType = operand.getType();

        if (obj == null) {
            return null;
        }

        // It's not possible to correctly wire InternalSerializationService into expressions themselves,
        // but it's possible to at least reuse same getter cache in every subsequent call.
        final Extractors extractors = Extractors.newBuilder(context.getSerializationService())
                .setGetterCacheSupplier(() -> getterCache)
                .build();
        final Map<String, Object> value = new HashMap<>();

        convert(obj, value, queryDataType, newSetFromMap(new IdentityHashMap<>()), extractors);

        return new HazelcastJsonValue(JsonCreationUtil.serializeValue(value));
    }

    private void convert(
            final Object source,
            final Map<String, Object> values,
            final QueryDataType dataType,
            final Set<Object> seenObjects,
            final Extractors extractors
    ) {
        if (!seenObjects.add(source)) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "Cycle detected in row value");
        }

        for (final QueryDataType.QueryDataTypeField field : dataType.getObjectFields()) {
            final Object fieldValue = extractors.extract(source, field.getName(), false);
            if (!field.getType().isCustomType() || fieldValue == null) {
                values.put(field.getName(), fieldValue);
            } else {
                final Map<String, Object> subFieldValue = new HashMap<>();
                values.put(field.getName(), subFieldValue);
                convert(fieldValue, subFieldValue, field.getType(), seenObjects, extractors);
            }
        }
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.UDT_OBJECT_TO_JSON;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
