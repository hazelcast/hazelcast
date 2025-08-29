/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.List;

/**
 * Value that is used for index lookups.
 * <p>
 * In the general case the value is composite. Every component stands for the respective component of the filter.
 * <p>
 * In addition, every component has "allowNull" flag that defines what to do in the case of NULL values. When
 * set to {@code false}, the observed NULL yields an empty result set immediately, e.g. for comparison
 * predicates. When set to {@code true}, the index is queried for null values, e.g. for {@code IS NULL}
 * predicate.
 * <p>
 * For example, for the composite filter {a, b} and the condition "WHERE a=1 AND b=2", the filter would be
 * {1/false, 2/false}. While for the condition "WHERE a=1 AND b IS NULL", the filter would be {1/false, null/true}.
 */
@SuppressWarnings("rawtypes")
public class IndexFilterValue implements IdentifiedDataSerializable {

    private List<Expression> components;
    private List<Boolean> allowNulls;

    public IndexFilterValue() {
        // No-op.
    }

    public IndexFilterValue(List<Expression> components, List<Boolean> allowNulls) {
        this.components = components;
        this.allowNulls = allowNulls;
    }

    public Comparable getValue(ExpressionEvalContext evalContext) {
        if (components.size() == 1) {
            return getComponentValue(0, evalContext);
        } else {
            Comparable[] componentValues = new Comparable[components.size()];

            for (int i = 0; i < components.size(); i++) {
                Comparable componentValue = getComponentValue(i, evalContext);

                if (componentValue == null) {
                    // One component returned NULL. It means that the filter will never return any entry, so there is no point
                    // to perform further evaluation. E.g. "WHERE a=NULL" or "WHERE a>1 AND a<NULL"
                    return null;
                }

                componentValues[i] = componentValue;
            }

            return new CompositeValue(componentValues);
        }
    }

    /**
     * Evaluate the value of the component at the given index.
     *
     * @param evalContext evaluation context
     * @return evaluated value or {@code null} if the evaluation should be stopped, because the parent index condition will
     * never return any entry
     */
    private Comparable getComponentValue(int index, ExpressionEvalContext evalContext) {
        Object value = components.get(index).evalTop(NoColumnAccessRow.INSTANCE, evalContext);

        if (value == null && allowNulls.get(index)) {
            // The evaluated value is NULL, but NULLs are allowed here (e.g. for "WHERE a IS NULL"). Return the special
            // NULL marker that will be used for the index lookup.
            value = AbstractIndex.NULL;
        }

        if (value != null && !(value instanceof Comparable)) {
            throw QueryException.dataException("Values used in index lookups must be Comparable: " + value);
        }

        return (Comparable) value;
    }

    public List<Expression> getComponents() {
        return components;
    }

    public List<Boolean> getAllowNulls() {
        return allowNulls;
    }

    public boolean isCooperative() {
        for (Expression<?> e : components) {
            if (!e.isCooperative()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INDEX_FILTER_VALUE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(components, out);
        SerializationUtil.writeList(allowNulls, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        components = SerializationUtil.readList(in);
        allowNulls = SerializationUtil.readList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexFilterValue value = (IndexFilterValue) o;

        return components.equals(value.components) && allowNulls.equals(value.allowNulls);
    }

    @Override
    public int hashCode() {
        int result = components.hashCode();

        result = 31 * result + allowNulls.hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "IndexFilterValue {components=" + components + ", allowNulls=" + allowNulls + '}';
    }

    /**
     * Special row implementation that disallows access to columns. Normally index filter should never access
     * columns. We pass this instance to index expressions to have a control over returned errors.
     */
    private static final class NoColumnAccessRow implements Row {

        private static final NoColumnAccessRow INSTANCE = new NoColumnAccessRow();

        @Override
        public <T> T get(int index) {
            throw new UnsupportedOperationException("Index filter cannot contain column expressions");
        }

        @Override
        public int getColumnCount() {
            throw new UnsupportedOperationException("Index filter cannot contain column expressions");
        }
    }
}
