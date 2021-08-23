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

package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.util.ExpressionType;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.BOOLEAN;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.BYTE;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.CHARACTER;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.DOUBLE;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.FLOAT;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.INTEGER;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.LONG;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.SHORT;
import static com.hazelcast.jet.sql.impl.util.ExpressionTypes.STRING;

abstract class JetSqlIndexTestSupport extends OptimizerTestSupport {

    @SuppressWarnings("rawtypes")
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

    protected static List<ExpressionType<?>> nonBaseTypes() {
        return Arrays.asList(
                BYTE,
                SHORT,
                LONG,
                BIG_DECIMAL,
                BIG_INTEGER,
                FLOAT,
                DOUBLE,
                CHARACTER
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
