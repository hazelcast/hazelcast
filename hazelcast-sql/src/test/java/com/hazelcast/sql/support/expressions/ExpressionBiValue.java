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

package com.hazelcast.sql.support.expressions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

@SuppressWarnings({"unused", "unchecked", "checkstyle:MultipleVariableDeclarations"})
public abstract class ExpressionBiValue extends ExpressionValue {
    public static Class<? extends ExpressionBiValue> createBiClass(ExpressionType<?> type1, ExpressionType<?> type2) {
        return createBiClass(type1.typeName(), type2.typeName());
    }

    public static Class<? extends ExpressionBiValue> createBiClass(String type1, String type2) {
        try {
            String className = ExpressionBiValue.class.getName() + "$" + type1 + type2 + "Val";

            return (Class<? extends ExpressionBiValue>) Class.forName(className);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot create " + ExpressionBiValue.class.getSimpleName() + " for types \""
                + type1 + "\" and \"" + type2 + "\"", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends ExpressionBiValue> T createBiValue(Class<? extends ExpressionBiValue> clazz) {
        try {
            return (T) clazz.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create an instance of " + clazz.getSimpleName());
        }
    }

    public static <T extends ExpressionBiValue> T createBiValue(
        Class<? extends ExpressionBiValue> clazz,
        int key,
        Object field1,
        Object field2
    ) {
        T res = create(clazz);

        res.key = key;
        res.field1(field1);
        res.field2(field2);

        return res;
    }

    public Object field2() {
        return getField("field2");
    }

    public ExpressionBiValue field2(Object value) {
        setField("field2", value);

        return this;
    }

    public ExpressionBiValue fields(Object value1, Object value2) {
        field1(value1);
        field2(value2);

        return this;
    }

    @Override
    public String toString() {
        return "[" + field1() + ", " + field2() + "]";
    }

    public static class BooleanBooleanVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Boolean field2; }
    public static class BooleanByteVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Byte field2; }
    public static class BooleanShortVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Short field2; }
    public static class BooleanIntegerVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Integer field2; }
    public static class BooleanLongVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Long field2; }
    public static class BooleanBigDecimalVal extends ExpressionBiValue implements Serializable { public Boolean field1; public BigDecimal field2; }
    public static class BooleanBigIntegerVal extends ExpressionBiValue implements Serializable { public Boolean field1; public BigInteger field2; }
    public static class BooleanFloatVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Float field2; }
    public static class BooleanDoubleVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Double field2; }
    public static class BooleanStringVal extends ExpressionBiValue implements Serializable { public Boolean field1; public String field2; }
    public static class BooleanCharacterVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Character field2; }
    public static class BooleanObjectVal extends ExpressionBiValue implements Serializable { public Boolean field1; public Object field2; }

    public static class ByteBooleanVal extends ExpressionBiValue implements Serializable { public Byte field1; public Boolean field2; }
    public static class ByteByteVal extends ExpressionBiValue implements Serializable { public Byte field1; public Byte field2; }
    public static class ByteShortVal extends ExpressionBiValue implements Serializable { public Byte field1; public Short field2; }
    public static class ByteIntegerVal extends ExpressionBiValue implements Serializable { public Byte field1; public Integer field2; }
    public static class ByteLongVal extends ExpressionBiValue implements Serializable { public Byte field1; public Long field2; }
    public static class ByteBigDecimalVal extends ExpressionBiValue implements Serializable { public Byte field1; public BigDecimal field2; }
    public static class ByteBigIntegerVal extends ExpressionBiValue implements Serializable { public Byte field1; public BigInteger field2; }
    public static class ByteFloatVal extends ExpressionBiValue implements Serializable { public Byte field1; public Float field2; }
    public static class ByteDoubleVal extends ExpressionBiValue implements Serializable { public Byte field1; public Double field2; }
    public static class ByteStringVal extends ExpressionBiValue implements Serializable { public Byte field1; public String field2; }
    public static class ByteCharacterVal extends ExpressionBiValue implements Serializable { public Byte field1; public Character field2; }
    public static class ByteObjectVal extends ExpressionBiValue implements Serializable { public Byte field1; public Object field2; }

    public static class ShortBooleanVal extends ExpressionBiValue implements Serializable { public Short field1; public Boolean field2; }
    public static class ShortByteVal extends ExpressionBiValue implements Serializable { public Short field1; public Byte field2; }
    public static class ShortShortVal extends ExpressionBiValue implements Serializable { public Short field1; public Short field2; }
    public static class ShortIntegerVal extends ExpressionBiValue implements Serializable { public Short field1; public Integer field2; }
    public static class ShortLongVal extends ExpressionBiValue implements Serializable { public Short field1; public Long field2; }
    public static class ShortBigDecimalVal extends ExpressionBiValue implements Serializable { public Short field1; public BigDecimal field2; }
    public static class ShortBigIntegerVal extends ExpressionBiValue implements Serializable { public Short field1; public BigInteger field2; }
    public static class ShortFloatVal extends ExpressionBiValue implements Serializable { public Short field1; public Float field2; }
    public static class ShortDoubleVal extends ExpressionBiValue implements Serializable { public Short field1; public Double field2; }
    public static class ShortStringVal extends ExpressionBiValue implements Serializable { public Short field1; public String field2; }
    public static class ShortCharacterVal extends ExpressionBiValue implements Serializable { public Short field1; public Character field2; }
    public static class ShortObjectVal extends ExpressionBiValue implements Serializable { public Short field1; public Object field2; }

    public static class IntegerBooleanVal extends ExpressionBiValue implements Serializable { public Integer field1; public Boolean field2; }
    public static class IntegerByteVal extends ExpressionBiValue implements Serializable { public Integer field1; public Byte field2; }
    public static class IntegerShortVal extends ExpressionBiValue implements Serializable { public Integer field1; public Short field2; }
    public static class IntegerIntegerVal extends ExpressionBiValue implements Serializable { public Integer field1; public Integer field2; }
    public static class IntegerLongVal extends ExpressionBiValue implements Serializable { public Integer field1; public Long field2; }
    public static class IntegerBigDecimalVal extends ExpressionBiValue implements Serializable { public Integer field1; public BigDecimal field2; }
    public static class IntegerBigIntegerVal extends ExpressionBiValue implements Serializable { public Integer field1; public BigInteger field2; }
    public static class IntegerFloatVal extends ExpressionBiValue implements Serializable { public Integer field1; public Float field2; }
    public static class IntegerDoubleVal extends ExpressionBiValue implements Serializable { public Integer field1; public Double field2; }
    public static class IntegerStringVal extends ExpressionBiValue implements Serializable { public Integer field1; public String field2; }
    public static class IntegerCharacterVal extends ExpressionBiValue implements Serializable { public Integer field1; public Character field2; }
    public static class IntegerObjectVal extends ExpressionBiValue implements Serializable { public Integer field1; public Object field2; }

    public static class LongBooleanVal extends ExpressionBiValue implements Serializable { public Long field1; public Boolean field2; }
    public static class LongByteVal extends ExpressionBiValue implements Serializable { public Long field1; public Byte field2; }
    public static class LongShortVal extends ExpressionBiValue implements Serializable { public Long field1; public Short field2; }
    public static class LongIntegerVal extends ExpressionBiValue implements Serializable { public Long field1; public Integer field2; }
    public static class LongLongVal extends ExpressionBiValue implements Serializable { public Long field1; public Long field2; }
    public static class LongBigDecimalVal extends ExpressionBiValue implements Serializable { public Long field1; public BigDecimal field2; }
    public static class LongBigIntegerVal extends ExpressionBiValue implements Serializable { public Long field1; public BigInteger field2; }
    public static class LongFloatVal extends ExpressionBiValue implements Serializable { public Long field1; public Float field2; }
    public static class LongDoubleVal extends ExpressionBiValue implements Serializable { public Long field1; public Double field2; }
    public static class LongStringVal extends ExpressionBiValue implements Serializable { public Long field1; public String field2; }
    public static class LongCharacterVal extends ExpressionBiValue implements Serializable { public Long field1; public Character field2; }
    public static class LongObjectVal extends ExpressionBiValue implements Serializable { public Long field1; public Object field2; }

    public static class BigDecimalBooleanVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Boolean field2; }
    public static class BigDecimalByteVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Byte field2; }
    public static class BigDecimalShortVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Short field2; }
    public static class BigDecimalIntegerVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Integer field2; }
    public static class BigDecimalLongVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Long field2; }
    public static class BigDecimalBigDecimalVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public BigDecimal field2; }
    public static class BigDecimalBigIntegerVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public BigInteger field2; }
    public static class BigDecimalFloatVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Float field2; }
    public static class BigDecimalDoubleVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Double field2; }
    public static class BigDecimalStringVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public String field2; }
    public static class BigDecimalCharacterVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Character field2; }
    public static class BigDecimalObjectVal extends ExpressionBiValue implements Serializable { public BigDecimal field1; public Object field2; }

    public static class BigIntegerBooleanVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Boolean field2; }
    public static class BigIntegerByteVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Byte field2; }
    public static class BigIntegerShortVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Short field2; }
    public static class BigIntegerIntegerVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Integer field2; }
    public static class BigIntegerLongVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Long field2; }
    public static class BigIntegerBigDecimalVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public BigDecimal field2; }
    public static class BigIntegerBigIntegerVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public BigInteger field2; }
    public static class BigIntegerFloatVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Float field2; }
    public static class BigIntegerDoubleVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Double field2; }
    public static class BigIntegerStringVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public String field2; }
    public static class BigIntegerCharacterVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Character field2; }
    public static class BigIntegerObjectVal extends ExpressionBiValue implements Serializable { public BigInteger field1; public Object field2; }

    public static class FloatBooleanVal extends ExpressionBiValue implements Serializable { public Float field1; public Boolean field2; }
    public static class FloatByteVal extends ExpressionBiValue implements Serializable { public Float field1; public Byte field2; }
    public static class FloatShortVal extends ExpressionBiValue implements Serializable { public Float field1; public Short field2; }
    public static class FloatIntegerVal extends ExpressionBiValue implements Serializable { public Float field1; public Integer field2; }
    public static class FloatLongVal extends ExpressionBiValue implements Serializable { public Float field1; public Long field2; }
    public static class FloatBigDecimalVal extends ExpressionBiValue implements Serializable { public Float field1; public BigDecimal field2; }
    public static class FloatBigIntegerVal extends ExpressionBiValue implements Serializable { public Float field1; public BigInteger field2; }
    public static class FloatFloatVal extends ExpressionBiValue implements Serializable { public Float field1; public Float field2; }
    public static class FloatDoubleVal extends ExpressionBiValue implements Serializable { public Float field1; public Double field2; }
    public static class FloatStringVal extends ExpressionBiValue implements Serializable { public Float field1; public String field2; }
    public static class FloatCharacterVal extends ExpressionBiValue implements Serializable { public Float field1; public Character field2; }
    public static class FloatObjectVal extends ExpressionBiValue implements Serializable { public Float field1; public Object field2; }

    public static class DoubleBooleanVal extends ExpressionBiValue implements Serializable { public Double field1; public Boolean field2; }
    public static class DoubleByteVal extends ExpressionBiValue implements Serializable { public Double field1; public Byte field2; }
    public static class DoubleShortVal extends ExpressionBiValue implements Serializable { public Double field1; public Short field2; }
    public static class DoubleIntegerVal extends ExpressionBiValue implements Serializable { public Double field1; public Integer field2; }
    public static class DoubleLongVal extends ExpressionBiValue implements Serializable { public Double field1; public Long field2; }
    public static class DoubleBigDecimalVal extends ExpressionBiValue implements Serializable { public Double field1; public BigDecimal field2; }
    public static class DoubleBigIntegerVal extends ExpressionBiValue implements Serializable { public Double field1; public BigInteger field2; }
    public static class DoubleFloatVal extends ExpressionBiValue implements Serializable { public Double field1; public Float field2; }
    public static class DoubleDoubleVal extends ExpressionBiValue implements Serializable { public Double field1; public Double field2; }
    public static class DoubleStringVal extends ExpressionBiValue implements Serializable { public Double field1; public String field2; }
    public static class DoubleCharacterVal extends ExpressionBiValue implements Serializable { public Double field1; public Character field2; }
    public static class DoubleObjectVal extends ExpressionBiValue implements Serializable { public Double field1; public Object field2; }

    public static class StringBooleanVal extends ExpressionBiValue implements Serializable { public String field1; public Boolean field2; }
    public static class StringByteVal extends ExpressionBiValue implements Serializable { public String field1; public Byte field2; }
    public static class StringShortVal extends ExpressionBiValue implements Serializable { public String field1; public Short field2; }
    public static class StringIntegerVal extends ExpressionBiValue implements Serializable { public String field1; public Integer field2; }
    public static class StringLongVal extends ExpressionBiValue implements Serializable { public String field1; public Long field2; }
    public static class StringBigDecimalVal extends ExpressionBiValue implements Serializable { public String field1; public BigDecimal field2; }
    public static class StringBigIntegerVal extends ExpressionBiValue implements Serializable { public String field1; public BigInteger field2; }
    public static class StringFloatVal extends ExpressionBiValue implements Serializable { public String field1; public Float field2; }
    public static class StringDoubleVal extends ExpressionBiValue implements Serializable { public String field1; public Double field2; }
    public static class StringStringVal extends ExpressionBiValue implements Serializable { public String field1; public String field2; }
    public static class StringCharacterVal extends ExpressionBiValue implements Serializable { public String field1; public Character field2; }
    public static class StringObjectVal extends ExpressionBiValue implements Serializable { public String field1; public Object field2; }

    public static class CharacterBooleanVal extends ExpressionBiValue implements Serializable { public Character field1; public Boolean field2; }
    public static class CharacterByteVal extends ExpressionBiValue implements Serializable { public Character field1; public Byte field2; }
    public static class CharacterShortVal extends ExpressionBiValue implements Serializable { public Character field1; public Short field2; }
    public static class CharacterIntegerVal extends ExpressionBiValue implements Serializable { public Character field1; public Integer field2; }
    public static class CharacterLongVal extends ExpressionBiValue implements Serializable { public Character field1; public Long field2; }
    public static class CharacterBigDecimalVal extends ExpressionBiValue implements Serializable { public Character field1; public BigDecimal field2; }
    public static class CharacterBigIntegerVal extends ExpressionBiValue implements Serializable { public Character field1; public BigInteger field2; }
    public static class CharacterFloatVal extends ExpressionBiValue implements Serializable { public Character field1; public Float field2; }
    public static class CharacterDoubleVal extends ExpressionBiValue implements Serializable { public Character field1; public Double field2; }
    public static class CharacterStringVal extends ExpressionBiValue implements Serializable { public Character field1; public String field2; }
    public static class CharacterCharacterVal extends ExpressionBiValue implements Serializable { public Character field1; public Character field2; }
    public static class CharacterObjectVal extends ExpressionBiValue implements Serializable { public Character field1; public Object field2; }

    public static class ObjectBooleanVal extends ExpressionBiValue implements Serializable { public Object field1; public Boolean field2; }
    public static class ObjectByteVal extends ExpressionBiValue implements Serializable { public Object field1; public Byte field2; }
    public static class ObjectShortVal extends ExpressionBiValue implements Serializable { public Object field1; public Short field2; }
    public static class ObjectIntegerVal extends ExpressionBiValue implements Serializable { public Object field1; public Integer field2; }
    public static class ObjectLongVal extends ExpressionBiValue implements Serializable { public Object field1; public Long field2; }
    public static class ObjectBigDecimalVal extends ExpressionBiValue implements Serializable { public Object field1; public BigDecimal field2; }
    public static class ObjectBigIntegerVal extends ExpressionBiValue implements Serializable { public Object field1; public BigInteger field2; }
    public static class ObjectFloatVal extends ExpressionBiValue implements Serializable { public Object field1; public Float field2; }
    public static class ObjectDoubleVal extends ExpressionBiValue implements Serializable { public Object field1; public Double field2; }
    public static class ObjectStringVal extends ExpressionBiValue implements Serializable { public Object field1; public String field2; }
    public static class ObjectCharacterVal extends ExpressionBiValue implements Serializable { public Object field1; public Character field2; }
    public static class ObjectObjectVal extends ExpressionBiValue implements Serializable { public Object field1; public Object field2; }
}
