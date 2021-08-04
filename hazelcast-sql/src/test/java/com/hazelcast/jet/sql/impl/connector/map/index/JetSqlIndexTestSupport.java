package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
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

public abstract class JetSqlIndexTestSupport extends OptimizerTestSupport {
    protected enum Usage {
        /** Index is not used */
        NONE,

        /** Only one component is used */
        ONE,

        /** Both components are used */
        BOTH
    }

    static String toLiteral(ExpressionType type, Object value) {
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

    static List<ExpressionType<?>> baseTypes() {
        return Arrays.asList(
            BOOLEAN,
            INTEGER,
            STRING
        );
    }

    static List<ExpressionType<?>> allTypes() {
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
