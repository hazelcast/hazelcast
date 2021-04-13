/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.index;

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionType;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.CHARACTER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.SHORT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.STRING;

@SuppressWarnings("rawtypes")
public class SqlIndexTestSupport extends SqlTestSupport {
    protected static String toLiteral(ExpressionType type, Object value) {
        if (type == BOOLEAN) {
            return Boolean.toString((Boolean) value);
        } else if (type == BYTE) {
            return Byte.toString((Byte) value);
        } else if (type == SHORT) {
            return Short.toString((Short) value);
        } else if (type == INTEGER) {
            return Integer.toString((Integer) value);
        } else if (type == LONG) {
            return Long.toString((Long) value);
        } else if (type == BIG_DECIMAL) {
            return value.toString();
        } else if (type == BIG_INTEGER) {
            return value.toString();
        } else if (type == FLOAT) {
            return Float.toString((Float) value);
        } else if (type == DOUBLE) {
            return Double.toString((Double) value);
        } else if (type == STRING) {
            return "'" + value + "'";
        } else if (type == CHARACTER) {
            return "'" + value + "'";
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    protected static List<ExpressionType<?>> baseTypes() {
        return Arrays.asList(
                BOOLEAN,
                INTEGER,
                STRING
        );
    }

    protected static List<ExpressionType<?>> allTypes() {
        return Arrays.asList(
                BOOLEAN,
                BYTE,
                SHORT,
                INTEGER,
                LONG,
                BIG_DECIMAL,
                BIG_INTEGER,
                FLOAT,
                DOUBLE,
                STRING,
                CHARACTER
        );
    }
}
