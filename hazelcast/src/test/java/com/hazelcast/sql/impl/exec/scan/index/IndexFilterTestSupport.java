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

import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.Collections;

/**
 * Utility stuff for index filter tests.
 */
@SuppressWarnings("rawtypes")
public class IndexFilterTestSupport extends SqlTestSupport {
    public static ConstantExpression constant(Object value, QueryDataType type) {
        return ConstantExpression.create(value, type);
    }

    public static CompositeValue composite(Comparable... components) {
        return new CompositeValue(components);
    }

    protected static IndexFilterValue intValue(Integer value) {
        return intValue(value, false);
    }

    protected static IndexFilterValue intValue(Integer value, boolean allowNull) {
        return new IndexFilterValue(
            Collections.singletonList(constant(value, QueryDataType.INT)),
            Collections.singletonList(allowNull)
        );
    }

    protected static IndexFilterValue intValues(Integer value1, Integer value2) {
        return intValues(value1, false, value2, false);
    }

    protected static IndexFilterValue intValues(
        Integer value1,
        boolean allowNull1,
        Integer value2,
        boolean allowNull2
    ) {
        return new IndexFilterValue(
            Arrays.asList(constant(value1, QueryDataType.INT), constant(value2, QueryDataType.INT)),
            Arrays.asList(allowNull1, allowNull2)
        );
    }
}
