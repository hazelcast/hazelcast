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

package com.hazelcast.sql.index;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.TestPlanNodeVisitorAdapter;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.Assert.assertNotNull;

@SuppressWarnings({"rawtypes", "unchecked", "unused", "checkstyle:MultipleVariableDeclarations"})
public class SqlIndexTestSupport extends SqlTestSupport {

    protected static final List<FieldDescriptor> FIELD_DESCRIPTORS;

    static {
        FIELD_DESCRIPTORS = Arrays.asList(
            new BooleanFieldDescriptor(),
            new ByteFieldDescriptor(),
            new ShortFieldDescriptor(),
            new IntegerFieldDescriptor(),
            new LongFieldDescriptor(),
            new BigDecimalFieldDescriptor(),
            new BigIntegerFieldDescriptor(),
            new FloatFieldDescriptor(),
            new DoubleFieldDescriptor(),
            new StringFieldDescriptor(),
            new CharacterFieldDescriptor()
        );
    }

    public static Predicate<Value> eq_1(Object operand) {
        return new ComparePredicate(Comparison.EQ, operand, true);
    }

    public static Predicate<Value> eq_2(Object operand) {
        return new ComparePredicate(Comparison.EQ, operand, false);
    }

    public static Predicate<Value> gt_1(Object operand) {
        return new ComparePredicate(Comparison.GT, operand, true);
    }

    public static Predicate<Value> gt_2(Object operand) {
        return new ComparePredicate(Comparison.GT, operand, false);
    }

    public static Predicate<Value> gte_1(Object operand) {
        return new ComparePredicate(Comparison.GTE, operand, true);
    }

    public static Predicate<Value> gte_2(Object operand) {
        return new ComparePredicate(Comparison.GTE, operand, false);
    }

    public static Predicate<Value> lt_1(Object operand) {
        return new ComparePredicate(Comparison.LT, operand, true);
    }

    public static Predicate<Value> lt_2(Object operand) {
        return new ComparePredicate(Comparison.LT, operand, false);
    }

    public static Predicate<Value> lte_1(Object operand) {
        return new ComparePredicate(Comparison.LTE, operand, true);
    }

    public static Predicate<Value> lte_2(Object operand) {
        return new ComparePredicate(Comparison.LTE, operand, false);
    }

    public static Predicate<Value> null_1() {
        return new NullPredicate(true);
    }

    public static Predicate<Value> null_2() {
        return new NullPredicate(false);
    }

    public static Predicate<Value> or(Predicate<Value>... predicates) {
        return new OrPredicate(predicates);
    }

    public static Predicate<Value> and(Predicate<Value>... predicates) {
        return new AndPredicate(predicates);
    }

    public Class<? extends Value> getValueClass(FieldDescriptor descriptor1, FieldDescriptor descriptor2) {
        try {
            return (Class<? extends Value>) Class.forName(
                SqlIndexTestSupport.class.getName() + "$" + descriptor1.typeName() + descriptor2.typeName()
            );
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create values class for " + descriptor1.getClass().getSimpleName()
                + " and " + descriptor2.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    protected static MapIndexScanPlanNode findIndexNode(SqlResult result) {
        SqlResultImpl result0 = (SqlResultImpl) result;

        AtomicReference<MapIndexScanPlanNode> nodeRef = new AtomicReference<>();

        for (int i = 0; i < result0.getPlan().getFragmentCount(); i++) {
            PlanNode fragment = result0.getPlan().getFragment(i);

            fragment.visit(new TestPlanNodeVisitorAdapter() {
                @Override
                public void onMapIndexScanNode(MapIndexScanPlanNode node) {
                    nodeRef.compareAndSet(null, node);

                    super.onMapIndexScanNode(node);
                }
            });
        }

        return nodeRef.get();
    }

    public enum Comparison {
        EQ, GT, GTE, LT, LTE
    }

    @SuppressWarnings("rawtypes")
    public static class ComparePredicate implements Predicate<Value> {

        private final Comparison comparison;
        private final Object operand;
        private final boolean firstField;

        public ComparePredicate(Comparison comparison, Object operand, boolean firstField) {
            assertNotNull("Use IsNullPredicate to compare with null", operand);

            this.comparison = comparison;
            this.operand = operand;
            this.firstField = firstField;
        }

        @Override
        public boolean test(Value value) {
            Object field = firstField ? value.getField1() : value.getField2();

            if (field == null) {
                return false;
            }

            int res = ((Comparable) field).compareTo(operand);

            switch (comparison) {
                case GT:
                    return res > 0;

                case GTE:
                    return res >= 0;

                case LT:
                    return res < 0;

                case LTE:
                    return res <= 0;

                default:
                    return res == 0;
            }
        }
    }

    public static class NullPredicate implements Predicate<Value> {

        private final boolean firstField;

        public NullPredicate(boolean firstField) {
            this.firstField = firstField;
        }

        @Override
        public boolean test(Value value) {
            Object field = firstField ? value.getField1() : value.getField2();

            return field == null;
        }
    }

    public static class OrPredicate implements Predicate<Value> {

        private final Predicate[] predicates;

        public OrPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
        public boolean test(Value value) {
            for (Predicate predicate : predicates) {
                if (predicate.test(value)) {
                    return true;
                }
            }

            return false;
        }
    }

    public static class AndPredicate implements Predicate<Value> {

        private final Predicate[] predicates;

        public AndPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
        public boolean test(Value value) {
            for (Predicate predicate : predicates) {
                if (!predicate.test(value)) {
                    return false;
                }
            }

            return true;
        }
    }

    public abstract static class FieldDescriptor<T> {
        protected abstract String typeName();
        protected abstract List<T> values();
        protected abstract T valueFrom();
        protected abstract T valueTo();
        protected abstract String toLiteral(Object value);
        protected abstract QueryDataType getFieldConverterType();

        protected Set<Object> parameterVariations(Object value) {
            if (value == null) {
                return Collections.singleton(null);
            }

            return parameterVariationsNotNull(value);
        }

        protected abstract Set<Object> parameterVariationsNotNull(Object value);

        @Override
        public String toString() {
            return typeName();
        }
    }

    public static class BooleanFieldDescriptor extends FieldDescriptor<Boolean>  {
        @Override
        protected String typeName() {
            return "Boolean";
        }

        @Override
        protected List<Boolean> values() {
            return Arrays.asList(true, false, null);
        }

        @Override
        protected Boolean valueFrom() {
            return false;
        }

        @Override
        protected Boolean valueTo() {
            return true;
        }

        @Override
        protected String toLiteral(Object value) {
            return Boolean.toString((Boolean) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            return new HashSet<>(Arrays.asList(
                value,
                value.toString()
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.BOOLEAN;
        }
    }

    public static class ByteFieldDescriptor extends FieldDescriptor<Byte> {
        @Override
        protected String typeName() {
            return "Byte";
        }

        @Override
        protected List<Byte> values() {
            return Arrays.asList((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, null);
        }

        @Override
        protected Byte valueFrom() {
            return (byte) 2;
        }

        @Override
        protected Byte valueTo() {
            return (byte) 4;
        }

        @Override
        protected String toLiteral(Object value) {
            return Byte.toString((Byte) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            byte value0 = (byte) value;

            return new HashSet<>(Arrays.asList(
                value0,
                (short) value0,
                (int) value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Byte.toString(value0)),
                new BigDecimal(Byte.toString(value0)),
                Byte.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.TINYINT;
        }
    }

    public static class ShortFieldDescriptor extends FieldDescriptor<Short> {
        @Override
        protected String typeName() {
            return "Short";
        }

        @Override
        protected List<Short> values() {
            return Arrays.asList((short) 1, (short) 2, (short) 3, (short) 4, (short) 5, null);
        }

        @Override
        protected Short valueFrom() {
            return (short) 2;
        }

        @Override
        protected Short valueTo() {
            return (short) 4;
        }

        @Override
        protected String toLiteral(Object value) {
            return Short.toString((Short) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            short value0 = (short) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                value0,
                (int) value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Short.toString(value0)),
                new BigDecimal(Short.toString(value0)),
                Short.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.SMALLINT;
        }
    }

    public static class IntegerFieldDescriptor extends FieldDescriptor<Integer> {
        @Override
        protected String typeName() {
            return "Integer";
        }

        @Override
        protected List<Integer> values() {
            return Arrays.asList(1, 2, 3, 4, 5, null);
        }

        @Override
        protected Integer valueFrom() {
            return 2;
        }

        @Override
        protected Integer valueTo() {
            return 4;
        }

        @Override
        protected String toLiteral(Object value) {
            return Integer.toString((Integer) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            int value0 = (int) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                value0,
                (long) value0,
                (float) value0,
                (double) value0,
                new BigInteger(Integer.toString(value0)),
                new BigDecimal(Integer.toString(value0)),
                Integer.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.INT;
        }
    }

    public static class LongFieldDescriptor extends FieldDescriptor<Long> {
        @Override
        protected String typeName() {
            return "Long";
        }

        @Override
        protected List<Long> values() {
            return Arrays.asList(1L, 2L, 3L, 4L, 5L, null);
        }

        @Override
        protected Long valueFrom() {
            return 2L;
        }

        @Override
        protected Long valueTo() {
            return 4L;
        }

        @Override
        protected String toLiteral(Object value) {
            return Long.toString((Long) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            long value0 = (long) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                value0,
                (float) value0,
                (double) value0,
                new BigInteger(Long.toString(value0)),
                new BigDecimal(Long.toString(value0)),
                Long.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.BIGINT;
        }
    }

    public static class BigDecimalFieldDescriptor extends FieldDescriptor<BigDecimal> {
        @Override
        protected String typeName() {
            return "BigDecimal";
        }

        @Override
        protected List<BigDecimal> values() {
            return Arrays.asList(
                new BigDecimal("1"),
                new BigDecimal("2"),
                new BigDecimal("3"),
                new BigDecimal("4"),
                new BigDecimal("5"),
                null
            );
        }

        @Override
        protected BigDecimal valueFrom() {
            return new BigDecimal("2");
        }

        @Override
        protected BigDecimal valueTo() {
            return new BigDecimal("4");
        }

        @Override
        protected String toLiteral(Object value) {
            return value.toString();
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            BigDecimal value0 = (BigDecimal) value;

            // TODO: float and double fails for HASH non-composite index (see commented)
            //   because the original BigDecimal "2" is not equal to BigDecimal "2.0"

            return new HashSet<>(Arrays.asList(
                value0.byteValue(),
                value0.shortValue(),
                value0.intValue(),
                value0.longValue(),
                // value0.floatValue(),
                // value0.doubleValue(),
                value0.toBigInteger(),
                value0,
                value0.toString()
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.DECIMAL;
        }
    }

    public static class BigIntegerFieldDescriptor extends FieldDescriptor<BigInteger> {
        @Override
        protected String typeName() {
            return "BigInteger";
        }

        @Override
        protected List<BigInteger> values() {
            return Arrays.asList(
                new BigInteger("1"),
                new BigInteger("2"),
                new BigInteger("3"),
                new BigInteger("4"),
                new BigInteger("5"),
                null
            );
        }

        @Override
        protected BigInteger valueFrom() {
            return new BigInteger("2");
        }

        @Override
        protected BigInteger valueTo() {
            return new BigInteger("4");
        }

        @Override
        protected String toLiteral(Object value) {
            return value.toString();
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            BigInteger value0 = (BigInteger) value;

            return new HashSet<>(Arrays.asList(
                value0.byteValue(),
                value0.shortValue(),
                value0.intValue(),
                value0.longValue(),
                value0.floatValue(),
                value0.doubleValue(),
                value0,
                new BigDecimal(value0),
                value0.toString()
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.DECIMAL_BIG_INTEGER;
        }
    }

    public static class FloatFieldDescriptor extends FieldDescriptor<Float> {
        @Override
        protected String typeName() {
            return "Float";
        }

        @Override
        protected List<Float> values() {
            return Arrays.asList(1f, 2f, 3f, 4f, 5f, null);
        }

        @Override
        protected Float valueFrom() {
            return 2f;
        }

        @Override
        protected Float valueTo() {
            return 4f;
        }

        @Override
        protected String toLiteral(Object value) {
            return Float.toString((Float) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            float value0 = (float) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                (long) value0,
                value0,
                (double) value0,
                new BigInteger(Integer.toString((int) value0)),
                new BigDecimal(Float.toString(value0)),
                Float.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.REAL;
        }
    }

    public static class DoubleFieldDescriptor extends FieldDescriptor<Double> {
        @Override
        protected String typeName() {
            return "Double";
        }

        @Override
        protected List<Double> values() {
            return Arrays.asList(1d, 2d, 3d, 4d, 5d, null);
        }

        @Override
        protected Double valueFrom() {
            return 2d;
        }

        @Override
        protected Double valueTo() {
            return 4d;
        }

        @Override
        protected String toLiteral(Object value) {
            return Double.toString((Double) value);
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            double value0 = (double) value;

            return new HashSet<>(Arrays.asList(
                (byte) value0,
                (short) value0,
                (int) value0,
                (long) value0,
                (float) value0,
                value0,
                new BigInteger(Integer.toString((int) value0)),
                new BigDecimal(Double.toString(value0)),
                Double.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.DOUBLE;
        }
    }

    public static class StringFieldDescriptor extends FieldDescriptor<String> {
        @Override
        protected String typeName() {
            return "String";
        }

        @Override
        protected List<String> values() {
            return Arrays.asList("a", "b", "c", "d", "e", null);
        }

        @Override
        protected String valueFrom() {
            return "b";
        }

        @Override
        protected String valueTo() {
            return "d";
        }

        @Override
        protected String toLiteral(Object value) {
            return "'" + value + "'";
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            return Collections.singleton(value);
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.VARCHAR;
        }
    }

    public static class CharacterFieldDescriptor extends FieldDescriptor<Character> {
        @Override
        protected String typeName() {
            return "Character";
        }

        @Override
        protected List<Character> values() {
            return Arrays.asList('a', 'b', 'c', 'd', 'e', null);
        }

        @Override
        protected Character valueFrom() {
            return 'b';
        }

        @Override
        protected Character valueTo() {
            return 'd';
        }

        @Override
        protected String toLiteral(Object value) {
            return "'" + value + "'";
        }

        @Override
        protected Set<Object> parameterVariationsNotNull(Object value) {
            char value0 = (char) value;

            return new HashSet<>(Arrays.asList(
                value0,
                Character.toString(value0)
            ));
        }

        @Override
        protected QueryDataType getFieldConverterType() {
            return QueryDataType.VARCHAR_CHARACTER;
        }
    }

    public static class Value {

        public int key;

        void setField1(Object value) {
            setField("field1", value);
        }

        void setField2(Object value) {
            setField("field2", value);
        }

        private void setField(String name, Object value) {
            try {
                getClass().getDeclaredField(name).set(this, value);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        Object getField1() {
            return getField("field1");
        }

        Object getField2() {
            return getField("field2");
        }

        private Object getField(String name) {
            try {
                return getClass().getDeclaredField(name).get(this);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "[" + getField1() + ", " + getField2() + "]";
        }
    }

    public static class BooleanBoolean extends Value implements Serializable { public Boolean field1; public Boolean field2; }
    public static class BooleanByte extends Value implements Serializable { public Boolean field1; public Byte field2; }
    public static class BooleanShort extends Value implements Serializable { public Boolean field1; public Short field2; }
    public static class BooleanInteger extends Value implements Serializable { public Boolean field1; public Integer field2; }
    public static class BooleanLong extends Value implements Serializable { public Boolean field1; public Long field2; }
    public static class BooleanBigDecimal extends Value implements Serializable { public Boolean field1; public BigDecimal field2; }
    public static class BooleanBigInteger extends Value implements Serializable { public Boolean field1; public BigInteger field2; }
    public static class BooleanFloat extends Value implements Serializable { public Boolean field1; public Float field2; }
    public static class BooleanDouble extends Value implements Serializable { public Boolean field1; public Double field2; }
    public static class BooleanString extends Value implements Serializable { public Boolean field1; public String field2; }
    public static class BooleanCharacter extends Value implements Serializable { public Boolean field1; public Character field2; }

    public static class ByteBoolean extends Value implements Serializable { public Byte field1; public Boolean field2; }
    public static class ByteByte extends Value implements Serializable { public Byte field1; public Byte field2; }
    public static class ByteShort extends Value implements Serializable { public Byte field1; public Short field2; }
    public static class ByteInteger extends Value implements Serializable { public Byte field1; public Integer field2; }
    public static class ByteLong extends Value implements Serializable { public Byte field1; public Long field2; }
    public static class ByteBigDecimal extends Value implements Serializable { public Byte field1; public BigDecimal field2; }
    public static class ByteBigInteger extends Value implements Serializable { public Byte field1; public BigInteger field2; }
    public static class ByteFloat extends Value implements Serializable { public Byte field1; public Float field2; }
    public static class ByteDouble extends Value implements Serializable { public Byte field1; public Double field2; }
    public static class ByteString extends Value implements Serializable { public Byte field1; public String field2; }
    public static class ByteCharacter extends Value implements Serializable { public Byte field1; public Character field2; }

    public static class ShortBoolean extends Value implements Serializable { public Short field1; public Boolean field2; }
    public static class ShortByte extends Value implements Serializable { public Short field1; public Byte field2; }
    public static class ShortShort extends Value implements Serializable { public Short field1; public Short field2; }
    public static class ShortInteger extends Value implements Serializable { public Short field1; public Integer field2; }
    public static class ShortLong extends Value implements Serializable { public Short field1; public Long field2; }
    public static class ShortBigDecimal extends Value implements Serializable { public Short field1; public BigDecimal field2; }
    public static class ShortBigInteger extends Value implements Serializable { public Short field1; public BigInteger field2; }
    public static class ShortFloat extends Value implements Serializable { public Short field1; public Float field2; }
    public static class ShortDouble extends Value implements Serializable { public Short field1; public Double field2; }
    public static class ShortString extends Value implements Serializable { public Short field1; public String field2; }
    public static class ShortCharacter extends Value implements Serializable { public Short field1; public Character field2; }

    public static class IntegerBoolean extends Value implements Serializable { public Integer field1; public Boolean field2; }
    public static class IntegerByte extends Value implements Serializable { public Integer field1; public Byte field2; }
    public static class IntegerShort extends Value implements Serializable { public Integer field1; public Short field2; }
    public static class IntegerInteger extends Value implements Serializable { public Integer field1; public Integer field2; }
    public static class IntegerLong extends Value implements Serializable { public Integer field1; public Long field2; }
    public static class IntegerBigDecimal extends Value implements Serializable { public Integer field1; public BigDecimal field2; }
    public static class IntegerBigInteger extends Value implements Serializable { public Integer field1; public BigInteger field2; }
    public static class IntegerFloat extends Value implements Serializable { public Integer field1; public Float field2; }
    public static class IntegerDouble extends Value implements Serializable { public Integer field1; public Double field2; }
    public static class IntegerString extends Value implements Serializable { public Integer field1; public String field2; }
    public static class IntegerCharacter extends Value implements Serializable { public Integer field1; public Character field2; }

    public static class LongBoolean extends Value implements Serializable { public Long field1; public Boolean field2; }
    public static class LongByte extends Value implements Serializable { public Long field1; public Byte field2; }
    public static class LongShort extends Value implements Serializable { public Long field1; public Short field2; }
    public static class LongInteger extends Value implements Serializable { public Long field1; public Integer field2; }
    public static class LongLong extends Value implements Serializable { public Long field1; public Long field2; }
    public static class LongBigDecimal extends Value implements Serializable { public Long field1; public BigDecimal field2; }
    public static class LongBigInteger extends Value implements Serializable { public Long field1; public BigInteger field2; }
    public static class LongFloat extends Value implements Serializable { public Long field1; public Float field2; }
    public static class LongDouble extends Value implements Serializable { public Long field1; public Double field2; }
    public static class LongString extends Value implements Serializable { public Long field1; public String field2; }
    public static class LongCharacter extends Value implements Serializable { public Long field1; public Character field2; }

    public static class BigDecimalBoolean extends Value implements Serializable { public BigDecimal field1; public Boolean field2; }
    public static class BigDecimalByte extends Value implements Serializable { public BigDecimal field1; public Byte field2; }
    public static class BigDecimalShort extends Value implements Serializable { public BigDecimal field1; public Short field2; }
    public static class BigDecimalInteger extends Value implements Serializable { public BigDecimal field1; public Integer field2; }
    public static class BigDecimalLong extends Value implements Serializable { public BigDecimal field1; public Long field2; }
    public static class BigDecimalBigDecimal extends Value implements Serializable { public BigDecimal field1; public BigDecimal field2; }
    public static class BigDecimalBigInteger extends Value implements Serializable { public BigDecimal field1; public BigInteger field2; }
    public static class BigDecimalFloat extends Value implements Serializable { public BigDecimal field1; public Float field2; }
    public static class BigDecimalDouble extends Value implements Serializable { public BigDecimal field1; public Double field2; }
    public static class BigDecimalString extends Value implements Serializable { public BigDecimal field1; public String field2; }
    public static class BigDecimalCharacter extends Value implements Serializable { public BigDecimal field1; public Character field2; }

    public static class BigIntegerBoolean extends Value implements Serializable { public BigInteger field1; public Boolean field2; }
    public static class BigIntegerByte extends Value implements Serializable { public BigInteger field1; public Byte field2; }
    public static class BigIntegerShort extends Value implements Serializable { public BigInteger field1; public Short field2; }
    public static class BigIntegerInteger extends Value implements Serializable { public BigInteger field1; public Integer field2; }
    public static class BigIntegerLong extends Value implements Serializable { public BigInteger field1; public Long field2; }
    public static class BigIntegerBigDecimal extends Value implements Serializable { public BigInteger field1; public BigDecimal field2; }
    public static class BigIntegerBigInteger extends Value implements Serializable { public BigInteger field1; public BigInteger field2; }
    public static class BigIntegerFloat extends Value implements Serializable { public BigInteger field1; public Float field2; }
    public static class BigIntegerDouble extends Value implements Serializable { public BigInteger field1; public Double field2; }
    public static class BigIntegerString extends Value implements Serializable { public BigInteger field1; public String field2; }
    public static class BigIntegerCharacter extends Value implements Serializable { public BigInteger field1; public Character field2; }

    public static class FloatBoolean extends Value implements Serializable { public Float field1; public Boolean field2; }
    public static class FloatByte extends Value implements Serializable { public Float field1; public Byte field2; }
    public static class FloatShort extends Value implements Serializable { public Float field1; public Short field2; }
    public static class FloatInteger extends Value implements Serializable { public Float field1; public Integer field2; }
    public static class FloatLong extends Value implements Serializable { public Float field1; public Long field2; }
    public static class FloatBigDecimal extends Value implements Serializable { public Float field1; public BigDecimal field2; }
    public static class FloatBigInteger extends Value implements Serializable { public Float field1; public BigInteger field2; }
    public static class FloatFloat extends Value implements Serializable { public Float field1; public Float field2; }
    public static class FloatDouble extends Value implements Serializable { public Float field1; public Double field2; }
    public static class FloatString extends Value implements Serializable { public Float field1; public String field2; }
    public static class FloatCharacter extends Value implements Serializable { public Float field1; public Character field2; }

    public static class DoubleBoolean extends Value implements Serializable { public Double field1; public Boolean field2; }
    public static class DoubleByte extends Value implements Serializable { public Double field1; public Byte field2; }
    public static class DoubleShort extends Value implements Serializable { public Double field1; public Short field2; }
    public static class DoubleInteger extends Value implements Serializable { public Double field1; public Integer field2; }
    public static class DoubleLong extends Value implements Serializable { public Double field1; public Long field2; }
    public static class DoubleBigDecimal extends Value implements Serializable { public Double field1; public BigDecimal field2; }
    public static class DoubleBigInteger extends Value implements Serializable { public Double field1; public BigInteger field2; }
    public static class DoubleFloat extends Value implements Serializable { public Double field1; public Float field2; }
    public static class DoubleDouble extends Value implements Serializable { public Double field1; public Double field2; }
    public static class DoubleString extends Value implements Serializable { public Double field1; public String field2; }
    public static class DoubleCharacter extends Value implements Serializable { public Double field1; public Character field2; }

    public static class StringBoolean extends Value implements Serializable { public String field1; public Boolean field2; }
    public static class StringByte extends Value implements Serializable { public String field1; public Byte field2; }
    public static class StringShort extends Value implements Serializable { public String field1; public Short field2; }
    public static class StringInteger extends Value implements Serializable { public String field1; public Integer field2; }
    public static class StringLong extends Value implements Serializable { public String field1; public Long field2; }
    public static class StringBigDecimal extends Value implements Serializable { public String field1; public BigDecimal field2; }
    public static class StringBigInteger extends Value implements Serializable { public String field1; public BigInteger field2; }
    public static class StringFloat extends Value implements Serializable { public String field1; public Float field2; }
    public static class StringDouble extends Value implements Serializable { public String field1; public Double field2; }
    public static class StringString extends Value implements Serializable { public String field1; public String field2; }
    public static class StringCharacter extends Value implements Serializable { public String field1; public Character field2; }

    public static class CharacterBoolean extends Value implements Serializable { public Character field1; public Boolean field2; }
    public static class CharacterByte extends Value implements Serializable { public Character field1; public Byte field2; }
    public static class CharacterShort extends Value implements Serializable { public Character field1; public Short field2; }
    public static class CharacterInteger extends Value implements Serializable { public Character field1; public Integer field2; }
    public static class CharacterLong extends Value implements Serializable { public Character field1; public Long field2; }
    public static class CharacterBigDecimal extends Value implements Serializable { public Character field1; public BigDecimal field2; }
    public static class CharacterBigInteger extends Value implements Serializable { public Character field1; public BigInteger field2; }
    public static class CharacterFloat extends Value implements Serializable { public Character field1; public Float field2; }
    public static class CharacterDouble extends Value implements Serializable { public Character field1; public Double field2; }
    public static class CharacterString extends Value implements Serializable { public Character field1; public String field2; }
    public static class CharacterCharacter extends Value implements Serializable { public Character field1; public Character field2; }
}
