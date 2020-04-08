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
 * Collector for the SINGLE_VALUE function.
 */
public class SingleValueAggregateCollector extends AggregateCollector {
    /** Marker for NULL value. */
    private static final Object NULL = new Object();

    /** Result. */
    private Object res;

    public SingleValueAggregateCollector(boolean distinct) {
        super(distinct);
    }

    @Override
    protected void collect0(Object operandValue, QueryDataType operandType) {
        if (res == null) {
            res = operandValue != null ? operandValue : NULL;
        } else {
            // TODO: User need more context here. But how to provide it?
            throw QueryException.error("Multiple values are not allowed for the group.");
        }
    }

    @Override
    public Object reduce() {
        return null;
    }
}
