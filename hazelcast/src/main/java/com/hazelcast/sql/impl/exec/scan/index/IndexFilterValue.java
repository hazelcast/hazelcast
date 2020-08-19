/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.List;

/**
 * Value that is used for filter lookup operations.
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
                    return null;
                }

                componentValues[i] = componentValue;
            }

            return new CompositeValue(componentValues);
        }
    }

    private Comparable getComponentValue(int index, ExpressionEvalContext evalContext) {
        Object value = components.get(index).eval(NoColumnAccessRow.INSTANCE, evalContext);

        if (value == null && allowNulls.get(index)) {
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

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.INDEX_FILTER_VALUE;
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
