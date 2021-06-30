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

import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;

import static java.util.Collections.singletonList;

public class MapIndexScanUtils {
    static IndexFilterValue intValue(Integer value) {
        return intValue(value, false);
    }

    static IndexFilterValue intValue(Integer value, boolean allowNull) {
        return new IndexFilterValue(
                singletonList(constant(value, QueryDataType.INT)),
                singletonList(allowNull)
        );
    }

    static IndexFilterValue shortValue(Short value) {
        return shortValue(value, false);
    }

    static IndexFilterValue shortValue(Short value, boolean allowNull) {
        return new IndexFilterValue(
                singletonList(constant(value, QueryDataType.SMALLINT)),
                singletonList(allowNull)
        );
    }

    static ConstantExpression constant(Object value, QueryDataType type) {
        return ConstantExpression.create(value, type);
    }
}
