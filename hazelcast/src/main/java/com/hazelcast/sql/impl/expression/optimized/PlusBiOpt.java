package com.hazelcast.sql.impl.expression.optimized;

import com.hazelcast.sql.impl.metadata.ExpressionType;

import java.math.BigDecimal;

public interface PlusBiOpt<T> extends BiOpt<T> {

    static <T> PlusBiOpt<T> forTypes(ExpressionType type1, ExpressionType type2) {
        ExpressionType biggerType;
        ExpressionType smallerType;

        if (type1.precedence() >= type2.precedence()) {
            biggerType = type1;
            smallerType = type2;
        }
        else {
            biggerType = type2;
            smallerType = type1;
        }

        return forTypes0(biggerType, smallerType);
    }

    @SuppressWarnings("unchecked")
    static <T> PlusBiOpt<T> forTypes0(ExpressionType biggerType, ExpressionType smallerType) {
        PlusBiOpt res;

        switch (biggerType.precedence()) {
            case ExpressionType.PRECEDENCE_TINYINT:
                res = BytePlusBiOpt.INSTANCE;

                break;

            case ExpressionType.PRECEDENCE_SMALLINT:
                res = ShortPlusBiOpt.INSTANCE;

                break;

            case ExpressionType.PRECEDENCE_INT:
                res = IntPlusBiOpt.INSTANCE;

                break;

            case ExpressionType.PRECEDENCE_BIGINT:
                res = LongPlusBiOpt.INSTANCE;

                break;

            case ExpressionType.PRECEDENCE_DECIMAL:
                res = BigDecimalPlusBiOpt.INSTANCE;

                break;

            case ExpressionType.PRECEDENCE_REAL:
            case ExpressionType.PRECEDENCE_DOUBLE:
                res = DoublePlusBiOpt.INSTANCE;

                break;

            default:
                throw new IllegalStateException("Should never happen.");
        }

        return (PlusBiOpt<T>)res;
    }

    class BytePlusBiOpt implements PlusBiOpt<Short> {
        private static final BytePlusBiOpt INSTANCE = new BytePlusBiOpt();

        @Override
        public Short eval(Object obj1, Object obj2) {
            int res = ((Byte)obj1).intValue() + (Byte)obj1;

            return (short)res;
        }
    }

    class ShortPlusBiOpt implements PlusBiOpt<Integer> {
        private static final ShortPlusBiOpt INSTANCE = new ShortPlusBiOpt();

        @Override
        public Integer eval(Object obj1, Object obj2) {
            int res = ((Short)obj1).intValue() + (Short)obj1;

            return (int)res;
        }
    }

    class IntPlusBiOpt implements PlusBiOpt<Long> {
        private static final IntPlusBiOpt INSTANCE = new IntPlusBiOpt();

        @Override
        public Long eval(Object obj1, Object obj2) {
            return ((Integer)obj1).longValue() + (Integer)obj1;
        }
    }

    class LongPlusBiOpt implements PlusBiOpt<BigDecimal> {
        private static final LongPlusBiOpt INSTANCE = new LongPlusBiOpt();

        @Override
        public BigDecimal eval(Object obj1, Object obj2) {
            BigDecimal res = new BigDecimal((Long)obj1);

            return res.add(new BigDecimal((Long)obj2));
        }
    }

    class BigDecimalPlusBiOpt implements PlusBiOpt<BigDecimal> {
        private static final BigDecimalPlusBiOpt INSTANCE = new BigDecimalPlusBiOpt();

        @Override
        public BigDecimal eval(Object obj1, Object obj2) {
            BigDecimal obj1Converted = (BigDecimal)obj1;
            BigDecimal obj2Converted = obj2 instanceof BigDecimal ? (BigDecimal)obj2 : new BigDecimal((Long)obj2);

            return obj1Converted.add(obj2Converted);
        }
    }

    class DoublePlusBiOpt implements PlusBiOpt<Double> {
        private static final DoublePlusBiOpt INSTANCE = new DoublePlusBiOpt();

        @Override
        public Double eval(Object obj1, Object obj2) {
            return ((Number)obj1).doubleValue() + ((Number)obj2).doubleValue();
        }
    }
}
