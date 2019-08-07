package com.hazelcast.sql.impl.expression.call.func;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.call.CallOperator;
import com.hazelcast.sql.impl.expression.call.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandomFunction extends UniCallExpression<Double> {
    /** Seed value type. */
    private transient DataType seedType;

    public RandomFunction() {
        // No-op.
    }

    public RandomFunction(Expression seedExp) {
        super(seedExp);
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Random random = getRandom(ctx, row);

        return random.nextDouble();
    }

    /**
     * Get random generator.
     *
     * @param ctx Context.
     * @param row Row.
     * @return Random generator.
     */
    private Random getRandom(QueryContext ctx, Row row) {
        Expression seedExp = operand;

        if (seedExp != null) {
            Object seedRes0 = seedExp.eval(ctx, row);

            if (seedRes0 != null) {
                if (seedType == null) {
                    DataType type = seedExp.getType();

                    if (!type.isCanConvertToNumeric())
                        throw new HazelcastSqlException(-1, "Seed is not numeric: " + seedExp);
                    
                    seedType = type;
                }

                int seedRes = seedType.getConverter().asInt(seedRes0);

                return new Random(seedRes);
            }
        }

        return ThreadLocalRandom.current();
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }

    @Override
    public int operator() {
        return CallOperator.RAND;
    }
}
