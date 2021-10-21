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

package com.hazelcast.jet.sql.impl.index;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.Collections;

/**
 * Utility stuff for index filter tests.
 */
@SuppressWarnings("rawtypes")
public abstract class IndexFilterTestSupport extends SqlTestSupport {

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
