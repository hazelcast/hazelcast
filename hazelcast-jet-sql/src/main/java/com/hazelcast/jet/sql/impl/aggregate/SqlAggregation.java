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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashSet;
import java.util.Set;

/**
 * A mutable object used for computing the SQL aggregation functions.
 */
public abstract class SqlAggregation implements DataSerializable {

    private int index;
    private boolean ignoreNulls;
    private Set<Object> values;

    protected SqlAggregation() {
    }

    protected SqlAggregation(int index, boolean ignoreNulls, boolean distinct) {
        this.index = index;
        this.ignoreNulls = ignoreNulls;
        this.values = distinct ? new HashSet<>() : null;
    }

    public abstract QueryDataType resultType();

    /**
     * Accumulate a value from a single row into this instance.
     */
    public final void accumulate(Object[] row) {
        Object value = index > -1 ? row[index] : null;

        if (value == null && ignoreNulls) {
            return;
        }

        if (value != null && values != null && !values.add(value)) {
            return;
        }

        accumulate(value);
    }

    protected abstract void accumulate(Object value);

    /**
     * Merge another aggregation into this aggregation.
     */
    public abstract void combine(SqlAggregation other);

    /**
     * Return the aggregation result.
     */
    public abstract Object collect();
}
