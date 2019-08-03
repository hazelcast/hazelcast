package com.hazelcast.sql.impl.expression.call;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.BaseDataTypeAccessor;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random function implementation.
 */
public class RandomFunction extends UniCallExpression<Double> {
    /** Seed value accessor. */
    private transient BaseDataTypeAccessor seedAccessor;

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
                // TODO: Common pattern.
                if (seedAccessor == null) {
                    DataType seedType = seedExp.getType();

                    if (!seedType.isNumeric())
                        throw new HazelcastSqlException(-1, "Seed is not numeric: " + seedExp);
                    
                    seedAccessor = seedType.getBaseType().getAccessor();
                }

                int seedRes = seedAccessor.getInt(seedRes0);

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
