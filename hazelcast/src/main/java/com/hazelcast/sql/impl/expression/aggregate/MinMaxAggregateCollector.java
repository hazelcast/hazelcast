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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Counting collector.
 */
public final class MinMaxAggregateCollector extends AggregateCollector {
    /** Min flag. */
    private final boolean min;

    /** Final result. */
    private Object res;

    MinMaxAggregateCollector(boolean min) {
        super(false);

        this.min = min;
    }

    @Override
    protected void collect0(Object operandValue, QueryDataType operandType) {
        if (res == null || (operandValue != null && replace(res, operandValue))) {
            res = operandValue;
        }
    }

    @Override
    public Object reduce() {
        return res;
    }

    @SuppressWarnings("unchecked")
    private boolean replace(Object oldVal, Object newVal) {
        assert oldVal != null;
        assert newVal != null;

        Comparable oldVal0 = asComparable(oldVal);
        Comparable newVal0 = asComparable(newVal);

        int res = newVal0.compareTo(oldVal0);

        return (min && res < 0) || (!min && res > 0);
    }

    private static Comparable<?> asComparable(Object val) {
        if (val instanceof Comparable) {
            return (Comparable<?>) val;
        } else {
            throw QueryException.error("Value is not comparable: " + val);
        }
    }
}
