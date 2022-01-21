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

package com.hazelcast.jet.sql.impl.support.expressions;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"unused", "unchecked", "checkstyle:MultipleVariableDeclarations"})
public abstract class ExpressionBiValue extends ExpressionValue {
    public static Class<? extends ExpressionBiValue> createBiClass(ExpressionType<?> type1, ExpressionType<?> type2) {
        return createBiClass(type1.typeName(), type2.typeName());
    }

    private static final ConcurrentHashMap<BiTuple<String, String>, Class<? extends ExpressionBiValue>> BI_CLASS_CACHE = new ConcurrentHashMap<>();

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public static Class<? extends ExpressionBiValue> biClassForType(QueryDataTypeFamily type1, QueryDataTypeFamily type2) {
        String typeName1 = Converters.getConverters().stream()
                .filter(c -> c.getTypeFamily() == type1)
                .findAny()
                .get()
                .getNormalizedValueClass()
                .getSimpleName();
        String typeName2 = Converters.getConverters().stream()
                .filter(c -> c.getTypeFamily() == type2)
                .findAny()
                .get()
                .getNormalizedValueClass()
                .getSimpleName();

        return createBiClass(typeName1, typeName2);
    }

    public static Class<? extends ExpressionBiValue> createBiClass(String type1, String type2) {
        return BI_CLASS_CACHE.computeIfAbsent(BiTuple.of(type1, type2), (k) -> createBiClass0(type1, type2));
    }

    public static Class<? extends ExpressionBiValue> createBiClass0(String type1, String type2) {
        try {
            String className = ExpressionBiValue.class.getName() + "$" + type1 + type2 + "Val";

            return (Class<? extends ExpressionBiValue>) Class.forName(className);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot create " + ExpressionBiValue.class.getSimpleName() + " for types \""
                    + type1 + "\" and \"" + type2 + "\"", e);
        }
    }

    public static <T extends ExpressionBiValue> T createBiValue(
            Object field1,
            Object field2
    ) {
        ExpressionType<?> type1 = ExpressionTypes.resolve(field1);
        ExpressionType<?> type2 = ExpressionTypes.resolve(field2);

        Class<? extends ExpressionBiValue> clazz = createBiClass(type1.typeName(), type2.typeName());

        return createBiValue(clazz, field1, field2);
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
            Object field1,
            Object field2
    ) {
        return createBiValue(clazz, 0, field1, field2);
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(field2());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        field2(in.readObject());
    }

    public static class BooleanBooleanVal extends ExpressionBiValue { public Boolean field1; public Boolean field2; }
    public static class BooleanByteVal extends ExpressionBiValue { public Boolean field1; public Byte field2; }
    public static class BooleanShortVal extends ExpressionBiValue { public Boolean field1; public Short field2; }
    public static class BooleanIntegerVal extends ExpressionBiValue { public Boolean field1; public Integer field2; }
    public static class BooleanLongVal extends ExpressionBiValue { public Boolean field1; public Long field2; }
    public static class BooleanBigDecimalVal extends ExpressionBiValue { public Boolean field1; public BigDecimal field2; }
    public static class BooleanBigIntegerVal extends ExpressionBiValue { public Boolean field1; public BigInteger field2; }
    public static class BooleanFloatVal extends ExpressionBiValue { public Boolean field1; public Float field2; }
    public static class BooleanDoubleVal extends ExpressionBiValue { public Boolean field1; public Double field2; }
    public static class BooleanStringVal extends ExpressionBiValue { public Boolean field1; public String field2; }
    public static class BooleanCharacterVal extends ExpressionBiValue { public Boolean field1; public Character field2; }
    public static class BooleanLocalDateVal extends ExpressionBiValue { public Boolean field1; public LocalDate field2; }
    public static class BooleanLocalTimeVal extends ExpressionBiValue { public Boolean field1; public LocalTime field2; }
    public static class BooleanLocalDateTimeVal extends ExpressionBiValue { public Boolean field1; public LocalDateTime field2; }
    public static class BooleanOffsetDateTimeVal extends ExpressionBiValue { public Boolean field1; public OffsetDateTime field2; }
    public static class BooleanObjectVal extends ExpressionBiValue { public Boolean field1; public Object field2; }
    public static class BooleanHazelcastJsonValueVal extends ExpressionBiValue { public Boolean field1; public HazelcastJsonValue field2; }

    public static class ByteBooleanVal extends ExpressionBiValue { public Byte field1; public Boolean field2; }
    public static class ByteByteVal extends ExpressionBiValue { public Byte field1; public Byte field2; }
    public static class ByteShortVal extends ExpressionBiValue { public Byte field1; public Short field2; }
    public static class ByteIntegerVal extends ExpressionBiValue { public Byte field1; public Integer field2; }
    public static class ByteLongVal extends ExpressionBiValue { public Byte field1; public Long field2; }
    public static class ByteBigDecimalVal extends ExpressionBiValue { public Byte field1; public BigDecimal field2; }
    public static class ByteBigIntegerVal extends ExpressionBiValue { public Byte field1; public BigInteger field2; }
    public static class ByteFloatVal extends ExpressionBiValue { public Byte field1; public Float field2; }
    public static class ByteDoubleVal extends ExpressionBiValue { public Byte field1; public Double field2; }
    public static class ByteStringVal extends ExpressionBiValue { public Byte field1; public String field2; }
    public static class ByteCharacterVal extends ExpressionBiValue { public Byte field1; public Character field2; }
    public static class ByteLocalDateVal extends ExpressionBiValue { public Byte field1; public LocalDate field2; }
    public static class ByteLocalTimeVal extends ExpressionBiValue { public Byte field1; public LocalTime field2; }
    public static class ByteLocalDateTimeVal extends ExpressionBiValue { public Byte field1; public LocalDateTime field2; }
    public static class ByteOffsetDateTimeVal extends ExpressionBiValue { public Byte field1; public OffsetDateTime field2; }
    public static class ByteObjectVal extends ExpressionBiValue { public Byte field1; public Object field2; }
    public static class ByteHazelcastJsonValueVal extends ExpressionBiValue { public Byte field1; public HazelcastJsonValue field2; }

    public static class ShortBooleanVal extends ExpressionBiValue { public Short field1; public Boolean field2; }
    public static class ShortByteVal extends ExpressionBiValue { public Short field1; public Byte field2; }
    public static class ShortShortVal extends ExpressionBiValue { public Short field1; public Short field2; }
    public static class ShortIntegerVal extends ExpressionBiValue { public Short field1; public Integer field2; }
    public static class ShortLongVal extends ExpressionBiValue { public Short field1; public Long field2; }
    public static class ShortBigDecimalVal extends ExpressionBiValue { public Short field1; public BigDecimal field2; }
    public static class ShortBigIntegerVal extends ExpressionBiValue { public Short field1; public BigInteger field2; }
    public static class ShortFloatVal extends ExpressionBiValue { public Short field1; public Float field2; }
    public static class ShortDoubleVal extends ExpressionBiValue { public Short field1; public Double field2; }
    public static class ShortStringVal extends ExpressionBiValue { public Short field1; public String field2; }
    public static class ShortCharacterVal extends ExpressionBiValue { public Short field1; public Character field2; }
    public static class ShortLocalDateVal extends ExpressionBiValue { public Short field1; public LocalDate field2; }
    public static class ShortLocalTimeVal extends ExpressionBiValue { public Short field1; public LocalTime field2; }
    public static class ShortLocalDateTimeVal extends ExpressionBiValue { public Short field1; public LocalDateTime field2; }
    public static class ShortOffsetDateTimeVal extends ExpressionBiValue { public Short field1; public OffsetDateTime field2; }
    public static class ShortObjectVal extends ExpressionBiValue { public Short field1; public Object field2; }
    public static class ShortHazelcastJsonValueVal extends ExpressionBiValue { public Short field1; public HazelcastJsonValue field2; }

    public static class IntegerBooleanVal extends ExpressionBiValue { public Integer field1; public Boolean field2; }
    public static class IntegerByteVal extends ExpressionBiValue { public Integer field1; public Byte field2; }
    public static class IntegerShortVal extends ExpressionBiValue { public Integer field1; public Short field2; }
    public static class IntegerIntegerVal extends ExpressionBiValue { public Integer field1; public Integer field2; }
    public static class IntegerLongVal extends ExpressionBiValue { public Integer field1; public Long field2; }
    public static class IntegerBigDecimalVal extends ExpressionBiValue { public Integer field1; public BigDecimal field2; }
    public static class IntegerBigIntegerVal extends ExpressionBiValue { public Integer field1; public BigInteger field2; }
    public static class IntegerFloatVal extends ExpressionBiValue { public Integer field1; public Float field2; }
    public static class IntegerDoubleVal extends ExpressionBiValue { public Integer field1; public Double field2; }
    public static class IntegerStringVal extends ExpressionBiValue { public Integer field1; public String field2; }
    public static class IntegerCharacterVal extends ExpressionBiValue { public Integer field1; public Character field2; }
    public static class IntegerLocalDateVal extends ExpressionBiValue { public Integer field1; public LocalDate field2; }
    public static class IntegerLocalTimeVal extends ExpressionBiValue { public Integer field1; public LocalTime field2; }
    public static class IntegerLocalDateTimeVal extends ExpressionBiValue { public Integer field1; public LocalDateTime field2; }
    public static class IntegerOffsetDateTimeVal extends ExpressionBiValue { public Integer field1; public OffsetDateTime field2; }
    public static class IntegerObjectVal extends ExpressionBiValue { public Integer field1; public Object field2; }
    public static class IntegerHazelcastJsonValueVal extends ExpressionBiValue { public Integer field1; public HazelcastJsonValue field2; }

    public static class LongBooleanVal extends ExpressionBiValue { public Long field1; public Boolean field2; }
    public static class LongByteVal extends ExpressionBiValue { public Long field1; public Byte field2; }
    public static class LongShortVal extends ExpressionBiValue { public Long field1; public Short field2; }
    public static class LongIntegerVal extends ExpressionBiValue { public Long field1; public Integer field2; }
    public static class LongLongVal extends ExpressionBiValue { public Long field1; public Long field2; }
    public static class LongBigDecimalVal extends ExpressionBiValue { public Long field1; public BigDecimal field2; }
    public static class LongBigIntegerVal extends ExpressionBiValue { public Long field1; public BigInteger field2; }
    public static class LongFloatVal extends ExpressionBiValue { public Long field1; public Float field2; }
    public static class LongDoubleVal extends ExpressionBiValue { public Long field1; public Double field2; }
    public static class LongStringVal extends ExpressionBiValue { public Long field1; public String field2; }
    public static class LongCharacterVal extends ExpressionBiValue { public Long field1; public Character field2; }
    public static class LongLocalDateVal extends ExpressionBiValue { public Long field1; public LocalDate field2; }
    public static class LongLocalTimeVal extends ExpressionBiValue { public Long field1; public LocalTime field2; }
    public static class LongLocalDateTimeVal extends ExpressionBiValue { public Long field1; public LocalDateTime field2; }
    public static class LongOffsetDateTimeVal extends ExpressionBiValue { public Long field1; public OffsetDateTime field2; }
    public static class LongObjectVal extends ExpressionBiValue { public Long field1; public Object field2; }
    public static class LongHazelcastJsonValueVal extends ExpressionBiValue { public Long field1; public HazelcastJsonValue field2; }

    public static class BigDecimalBooleanVal extends ExpressionBiValue { public BigDecimal field1; public Boolean field2; }
    public static class BigDecimalByteVal extends ExpressionBiValue { public BigDecimal field1; public Byte field2; }
    public static class BigDecimalShortVal extends ExpressionBiValue { public BigDecimal field1; public Short field2; }
    public static class BigDecimalIntegerVal extends ExpressionBiValue { public BigDecimal field1; public Integer field2; }
    public static class BigDecimalLongVal extends ExpressionBiValue { public BigDecimal field1; public Long field2; }
    public static class BigDecimalBigDecimalVal extends ExpressionBiValue { public BigDecimal field1; public BigDecimal field2; }
    public static class BigDecimalBigIntegerVal extends ExpressionBiValue { public BigDecimal field1; public BigInteger field2; }
    public static class BigDecimalFloatVal extends ExpressionBiValue { public BigDecimal field1; public Float field2; }
    public static class BigDecimalDoubleVal extends ExpressionBiValue { public BigDecimal field1; public Double field2; }
    public static class BigDecimalStringVal extends ExpressionBiValue { public BigDecimal field1; public String field2; }
    public static class BigDecimalCharacterVal extends ExpressionBiValue { public BigDecimal field1; public Character field2; }
    public static class BigDecimalLocalDateVal extends ExpressionBiValue { public BigDecimal field1; public LocalDate field2; }
    public static class BigDecimalLocalTimeVal extends ExpressionBiValue { public BigDecimal field1; public LocalTime field2; }
    public static class BigDecimalLocalDateTimeVal extends ExpressionBiValue { public BigDecimal field1; public LocalDateTime field2; }
    public static class BigDecimalOffsetDateTimeVal extends ExpressionBiValue { public BigDecimal field1; public OffsetDateTime field2; }
    public static class BigDecimalObjectVal extends ExpressionBiValue { public BigDecimal field1; public Object field2; }
    public static class BigDecimalHazelcastJsonValueVal extends ExpressionBiValue { public BigDecimal field1; public HazelcastJsonValue field2; }

    public static class BigIntegerBooleanVal extends ExpressionBiValue { public BigInteger field1; public Boolean field2; }
    public static class BigIntegerByteVal extends ExpressionBiValue { public BigInteger field1; public Byte field2; }
    public static class BigIntegerShortVal extends ExpressionBiValue { public BigInteger field1; public Short field2; }
    public static class BigIntegerIntegerVal extends ExpressionBiValue { public BigInteger field1; public Integer field2; }
    public static class BigIntegerLongVal extends ExpressionBiValue { public BigInteger field1; public Long field2; }
    public static class BigIntegerBigDecimalVal extends ExpressionBiValue { public BigInteger field1; public BigDecimal field2; }
    public static class BigIntegerBigIntegerVal extends ExpressionBiValue { public BigInteger field1; public BigInteger field2; }
    public static class BigIntegerFloatVal extends ExpressionBiValue { public BigInteger field1; public Float field2; }
    public static class BigIntegerDoubleVal extends ExpressionBiValue { public BigInteger field1; public Double field2; }
    public static class BigIntegerStringVal extends ExpressionBiValue { public BigInteger field1; public String field2; }
    public static class BigIntegerCharacterVal extends ExpressionBiValue { public BigInteger field1; public Character field2; }
    public static class BigIntegerLocalDateVal extends ExpressionBiValue { public BigInteger field1; public LocalDate field2; }
    public static class BigIntegerLocalTimeVal extends ExpressionBiValue { public BigInteger field1; public LocalTime field2; }
    public static class BigIntegerLocalDateTimeVal extends ExpressionBiValue { public BigInteger field1; public LocalDateTime field2; }
    public static class BigIntegerOffsetDateTimeVal extends ExpressionBiValue { public BigInteger field1; public OffsetDateTime field2; }
    public static class BigIntegerObjectVal extends ExpressionBiValue { public BigInteger field1; public Object field2; }
    public static class BigIntegerHazelcastJsonValueVal extends ExpressionBiValue { public BigInteger field1; public HazelcastJsonValue field2; }

    public static class FloatBooleanVal extends ExpressionBiValue { public Float field1; public Boolean field2; }
    public static class FloatByteVal extends ExpressionBiValue { public Float field1; public Byte field2; }
    public static class FloatShortVal extends ExpressionBiValue { public Float field1; public Short field2; }
    public static class FloatIntegerVal extends ExpressionBiValue { public Float field1; public Integer field2; }
    public static class FloatLongVal extends ExpressionBiValue { public Float field1; public Long field2; }
    public static class FloatBigDecimalVal extends ExpressionBiValue { public Float field1; public BigDecimal field2; }
    public static class FloatBigIntegerVal extends ExpressionBiValue { public Float field1; public BigInteger field2; }
    public static class FloatFloatVal extends ExpressionBiValue { public Float field1; public Float field2; }
    public static class FloatDoubleVal extends ExpressionBiValue { public Float field1; public Double field2; }
    public static class FloatStringVal extends ExpressionBiValue { public Float field1; public String field2; }
    public static class FloatCharacterVal extends ExpressionBiValue { public Float field1; public Character field2; }
    public static class FloatLocalDateVal extends ExpressionBiValue { public Float field1; public LocalDate field2; }
    public static class FloatLocalTimeVal extends ExpressionBiValue { public Float field1; public LocalTime field2; }
    public static class FloatLocalDateTimeVal extends ExpressionBiValue { public Float field1; public LocalDateTime field2; }
    public static class FloatOffsetDateTimeVal extends ExpressionBiValue { public Float field1; public OffsetDateTime field2; }
    public static class FloatObjectVal extends ExpressionBiValue { public Float field1; public Object field2; }
    public static class FloatHazelcastJsonValueVal extends ExpressionBiValue { public Float field1; public HazelcastJsonValue field2; }

    public static class DoubleBooleanVal extends ExpressionBiValue { public Double field1; public Boolean field2; }
    public static class DoubleByteVal extends ExpressionBiValue { public Double field1; public Byte field2; }
    public static class DoubleShortVal extends ExpressionBiValue { public Double field1; public Short field2; }
    public static class DoubleIntegerVal extends ExpressionBiValue { public Double field1; public Integer field2; }
    public static class DoubleLongVal extends ExpressionBiValue { public Double field1; public Long field2; }
    public static class DoubleBigDecimalVal extends ExpressionBiValue { public Double field1; public BigDecimal field2; }
    public static class DoubleBigIntegerVal extends ExpressionBiValue { public Double field1; public BigInteger field2; }
    public static class DoubleFloatVal extends ExpressionBiValue { public Double field1; public Float field2; }
    public static class DoubleDoubleVal extends ExpressionBiValue { public Double field1; public Double field2; }
    public static class DoubleStringVal extends ExpressionBiValue { public Double field1; public String field2; }
    public static class DoubleCharacterVal extends ExpressionBiValue { public Double field1; public Character field2; }
    public static class DoubleLocalDateVal extends ExpressionBiValue { public Double field1; public LocalDate field2; }
    public static class DoubleLocalTimeVal extends ExpressionBiValue { public Double field1; public LocalTime field2; }
    public static class DoubleLocalDateTimeVal extends ExpressionBiValue { public Double field1; public LocalDateTime field2; }
    public static class DoubleOffsetDateTimeVal extends ExpressionBiValue { public Double field1; public OffsetDateTime field2; }
    public static class DoubleObjectVal extends ExpressionBiValue { public Double field1; public Object field2; }
    public static class DoubleHazelcastJsonValueVal extends ExpressionBiValue { public Double field1; public HazelcastJsonValue field2; }

    public static class StringBooleanVal extends ExpressionBiValue { public String field1; public Boolean field2; }
    public static class StringByteVal extends ExpressionBiValue { public String field1; public Byte field2; }
    public static class StringShortVal extends ExpressionBiValue { public String field1; public Short field2; }
    public static class StringIntegerVal extends ExpressionBiValue { public String field1; public Integer field2; }
    public static class StringLongVal extends ExpressionBiValue { public String field1; public Long field2; }
    public static class StringBigDecimalVal extends ExpressionBiValue { public String field1; public BigDecimal field2; }
    public static class StringBigIntegerVal extends ExpressionBiValue { public String field1; public BigInteger field2; }
    public static class StringFloatVal extends ExpressionBiValue { public String field1; public Float field2; }
    public static class StringDoubleVal extends ExpressionBiValue { public String field1; public Double field2; }
    public static class StringStringVal extends ExpressionBiValue { public String field1; public String field2; }
    public static class StringCharacterVal extends ExpressionBiValue { public String field1; public Character field2; }
    public static class StringLocalDateVal extends ExpressionBiValue { public String field1; public LocalDate field2; }
    public static class StringLocalTimeVal extends ExpressionBiValue { public String field1; public LocalTime field2; }
    public static class StringLocalDateTimeVal extends ExpressionBiValue { public String field1; public LocalDateTime field2; }
    public static class StringOffsetDateTimeVal extends ExpressionBiValue { public String field1; public OffsetDateTime field2; }
    public static class StringObjectVal extends ExpressionBiValue { public String field1; public Object field2; }
    public static class StringHazelcastJsonValueVal extends ExpressionBiValue { public String field1; public HazelcastJsonValue field2; }

    public static class CharacterBooleanVal extends ExpressionBiValue { public Character field1; public Boolean field2; }
    public static class CharacterByteVal extends ExpressionBiValue { public Character field1; public Byte field2; }
    public static class CharacterShortVal extends ExpressionBiValue { public Character field1; public Short field2; }
    public static class CharacterIntegerVal extends ExpressionBiValue { public Character field1; public Integer field2; }
    public static class CharacterLongVal extends ExpressionBiValue { public Character field1; public Long field2; }
    public static class CharacterBigDecimalVal extends ExpressionBiValue { public Character field1; public BigDecimal field2; }
    public static class CharacterBigIntegerVal extends ExpressionBiValue { public Character field1; public BigInteger field2; }
    public static class CharacterFloatVal extends ExpressionBiValue { public Character field1; public Float field2; }
    public static class CharacterDoubleVal extends ExpressionBiValue { public Character field1; public Double field2; }
    public static class CharacterStringVal extends ExpressionBiValue { public Character field1; public String field2; }
    public static class CharacterCharacterVal extends ExpressionBiValue { public Character field1; public Character field2; }
    public static class CharacterLocalDateVal extends ExpressionBiValue { public Character field1; public LocalDate field2; }
    public static class CharacterLocalTimeVal extends ExpressionBiValue { public Character field1; public LocalTime field2; }
    public static class CharacterLocalDateTimeVal extends ExpressionBiValue { public Character field1; public LocalDateTime field2; }
    public static class CharacterOffsetDateTimeVal extends ExpressionBiValue { public Character field1; public OffsetDateTime field2; }
    public static class CharacterObjectVal extends ExpressionBiValue { public Character field1; public Object field2; }
    public static class CharacterHazelcastJsonValueVal extends ExpressionBiValue { public Character field1; public HazelcastJsonValue field2; }

    public static class LocalDateBooleanVal extends ExpressionBiValue { public LocalDate field1; public Boolean field2; }
    public static class LocalDateByteVal extends ExpressionBiValue { public LocalDate field1; public Byte field2; }
    public static class LocalDateShortVal extends ExpressionBiValue { public LocalDate field1; public Short field2; }
    public static class LocalDateIntegerVal extends ExpressionBiValue { public LocalDate field1; public Integer field2; }
    public static class LocalDateLongVal extends ExpressionBiValue { public LocalDate field1; public Long field2; }
    public static class LocalDateBigDecimalVal extends ExpressionBiValue { public LocalDate field1; public BigDecimal field2; }
    public static class LocalDateBigIntegerVal extends ExpressionBiValue { public LocalDate field1; public BigInteger field2; }
    public static class LocalDateFloatVal extends ExpressionBiValue { public LocalDate field1; public Float field2; }
    public static class LocalDateDoubleVal extends ExpressionBiValue { public LocalDate field1; public Double field2; }
    public static class LocalDateStringVal extends ExpressionBiValue { public LocalDate field1; public String field2; }
    public static class LocalDateCharacterVal extends ExpressionBiValue { public LocalDate field1; public Character field2; }
    public static class LocalDateLocalDateVal extends ExpressionBiValue { public LocalDate field1; public LocalDate field2; }
    public static class LocalDateLocalTimeVal extends ExpressionBiValue { public LocalDate field1; public LocalTime field2; }
    public static class LocalDateLocalDateTimeVal extends ExpressionBiValue { public LocalDate field1; public LocalDateTime field2; }
    public static class LocalDateOffsetDateTimeVal extends ExpressionBiValue { public LocalDate field1; public OffsetDateTime field2; }
    public static class LocalDateObjectVal extends ExpressionBiValue { public LocalDate field1; public Object field2; }
    public static class LocalDateHazelcastJsonValueVal extends ExpressionBiValue { public LocalDate field1; public HazelcastJsonValue field2; }

    public static class LocalTimeBooleanVal extends ExpressionBiValue { public LocalTime field1; public Boolean field2; }
    public static class LocalTimeByteVal extends ExpressionBiValue { public LocalTime field1; public Byte field2; }
    public static class LocalTimeShortVal extends ExpressionBiValue { public LocalTime field1; public Short field2; }
    public static class LocalTimeIntegerVal extends ExpressionBiValue { public LocalTime field1; public Integer field2; }
    public static class LocalTimeLongVal extends ExpressionBiValue { public LocalTime field1; public Long field2; }
    public static class LocalTimeBigDecimalVal extends ExpressionBiValue { public LocalTime field1; public BigDecimal field2; }
    public static class LocalTimeBigIntegerVal extends ExpressionBiValue { public LocalTime field1; public BigInteger field2; }
    public static class LocalTimeFloatVal extends ExpressionBiValue { public LocalTime field1; public Float field2; }
    public static class LocalTimeDoubleVal extends ExpressionBiValue { public LocalTime field1; public Double field2; }
    public static class LocalTimeStringVal extends ExpressionBiValue { public LocalTime field1; public String field2; }
    public static class LocalTimeCharacterVal extends ExpressionBiValue { public LocalTime field1; public Character field2; }
    public static class LocalTimeLocalDateVal extends ExpressionBiValue { public LocalTime field1; public LocalDate field2; }
    public static class LocalTimeLocalTimeVal extends ExpressionBiValue { public LocalTime field1; public LocalTime field2; }
    public static class LocalTimeLocalDateTimeVal extends ExpressionBiValue { public LocalTime field1; public LocalDateTime field2; }
    public static class LocalTimeOffsetDateTimeVal extends ExpressionBiValue { public LocalTime field1; public OffsetDateTime field2; }
    public static class LocalTimeObjectVal extends ExpressionBiValue { public LocalTime field1; public Object field2; }
    public static class LocalTimeHazelcastJsonValueVal extends ExpressionBiValue { public LocalTime field1; public HazelcastJsonValue field2; }

    public static class LocalDateTimeBooleanVal extends ExpressionBiValue { public LocalDateTime field1; public Boolean field2; }
    public static class LocalDateTimeByteVal extends ExpressionBiValue { public LocalDateTime field1; public Byte field2; }
    public static class LocalDateTimeShortVal extends ExpressionBiValue { public LocalDateTime field1; public Short field2; }
    public static class LocalDateTimeIntegerVal extends ExpressionBiValue { public LocalDateTime field1; public Integer field2; }
    public static class LocalDateTimeLongVal extends ExpressionBiValue { public LocalDateTime field1; public Long field2; }
    public static class LocalDateTimeBigDecimalVal extends ExpressionBiValue { public LocalDateTime field1; public BigDecimal field2; }
    public static class LocalDateTimeBigIntegerVal extends ExpressionBiValue { public LocalDateTime field1; public BigInteger field2; }
    public static class LocalDateTimeFloatVal extends ExpressionBiValue { public LocalDateTime field1; public Float field2; }
    public static class LocalDateTimeDoubleVal extends ExpressionBiValue { public LocalDateTime field1; public Double field2; }
    public static class LocalDateTimeStringVal extends ExpressionBiValue { public LocalDateTime field1; public String field2; }
    public static class LocalDateTimeCharacterVal extends ExpressionBiValue { public LocalDateTime field1; public Character field2; }
    public static class LocalDateTimeLocalDateVal extends ExpressionBiValue { public LocalDateTime field1; public LocalDate field2; }
    public static class LocalDateTimeLocalTimeVal extends ExpressionBiValue { public LocalDateTime field1; public LocalTime field2; }
    public static class LocalDateTimeLocalDateTimeVal extends ExpressionBiValue { public LocalDateTime field1; public LocalDateTime field2; }
    public static class LocalDateTimeOffsetDateTimeVal extends ExpressionBiValue { public LocalDateTime field1; public OffsetDateTime field2; }
    public static class LocalDateTimeObjectVal extends ExpressionBiValue { public LocalDateTime field1; public Object field2; }
    public static class LocalDateTimeHazelcastJsonValueVal extends ExpressionBiValue { public LocalDateTime field1; public HazelcastJsonValue field2; }

    public static class OffsetDateTimeBooleanVal extends ExpressionBiValue { public OffsetDateTime field1; public Boolean field2; }
    public static class OffsetDateTimeByteVal extends ExpressionBiValue { public OffsetDateTime field1; public Byte field2; }
    public static class OffsetDateTimeShortVal extends ExpressionBiValue { public OffsetDateTime field1; public Short field2; }
    public static class OffsetDateTimeIntegerVal extends ExpressionBiValue { public OffsetDateTime field1; public Integer field2; }
    public static class OffsetDateTimeLongVal extends ExpressionBiValue { public OffsetDateTime field1; public Long field2; }
    public static class OffsetDateTimeBigDecimalVal extends ExpressionBiValue { public OffsetDateTime field1; public BigDecimal field2; }
    public static class OffsetDateTimeBigIntegerVal extends ExpressionBiValue { public OffsetDateTime field1; public BigInteger field2; }
    public static class OffsetDateTimeFloatVal extends ExpressionBiValue { public OffsetDateTime field1; public Float field2; }
    public static class OffsetDateTimeDoubleVal extends ExpressionBiValue { public OffsetDateTime field1; public Double field2; }
    public static class OffsetDateTimeStringVal extends ExpressionBiValue { public OffsetDateTime field1; public String field2; }
    public static class OffsetDateTimeCharacterVal extends ExpressionBiValue { public OffsetDateTime field1; public Character field2; }
    public static class OffsetDateTimeLocalDateVal extends ExpressionBiValue { public OffsetDateTime field1; public LocalDate field2; }
    public static class OffsetDateTimeLocalTimeVal extends ExpressionBiValue { public OffsetDateTime field1; public LocalTime field2; }
    public static class OffsetDateTimeLocalDateTimeVal extends ExpressionBiValue { public OffsetDateTime field1; public LocalDateTime field2; }
    public static class OffsetDateTimeOffsetDateTimeVal extends ExpressionBiValue { public OffsetDateTime field1; public OffsetDateTime field2; }
    public static class OffsetDateTimeObjectVal extends ExpressionBiValue { public OffsetDateTime field1; public Object field2; }
    public static class OffsetDateTimeHazelcastJsonValueVal extends ExpressionBiValue { public OffsetDateTime field1; public HazelcastJsonValue field2; }

    public static class ObjectBooleanVal extends ExpressionBiValue { public Object field1; public Boolean field2; }
    public static class ObjectByteVal extends ExpressionBiValue { public Object field1; public Byte field2; }
    public static class ObjectShortVal extends ExpressionBiValue { public Object field1; public Short field2; }
    public static class ObjectIntegerVal extends ExpressionBiValue { public Object field1; public Integer field2; }
    public static class ObjectLongVal extends ExpressionBiValue { public Object field1; public Long field2; }
    public static class ObjectBigDecimalVal extends ExpressionBiValue { public Object field1; public BigDecimal field2; }
    public static class ObjectBigIntegerVal extends ExpressionBiValue { public Object field1; public BigInteger field2; }
    public static class ObjectFloatVal extends ExpressionBiValue { public Object field1; public Float field2; }
    public static class ObjectDoubleVal extends ExpressionBiValue { public Object field1; public Double field2; }
    public static class ObjectStringVal extends ExpressionBiValue { public Object field1; public String field2; }
    public static class ObjectCharacterVal extends ExpressionBiValue { public Object field1; public Character field2; }
    public static class ObjectLocalDateVal extends ExpressionBiValue { public Object field1; public LocalDate field2; }
    public static class ObjectLocalTimeVal extends ExpressionBiValue { public Object field1; public LocalTime field2; }
    public static class ObjectLocalDateTimeVal extends ExpressionBiValue { public Object field1; public LocalDateTime field2; }
    public static class ObjectOffsetDateTimeVal extends ExpressionBiValue { public Object field1; public OffsetDateTime field2; }
    public static class ObjectObjectVal extends ExpressionBiValue { public Object field1; public Object field2; }
    public static class ObjectHazelcastJsonValueVal extends ExpressionBiValue { public Object field1; public HazelcastJsonValue field2; }

    public static class HazelcastJsonValueBooleanVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Boolean field2; }
    public static class HazelcastJsonValueByteVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Byte field2; }
    public static class HazelcastJsonValueShortVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Short field2; }
    public static class HazelcastJsonValueIntegerVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Integer field2; }
    public static class HazelcastJsonValueLongVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Long field2; }
    public static class HazelcastJsonValueBigDecimalVal extends ExpressionBiValue { public HazelcastJsonValue field1; public BigDecimal field2; }
    public static class HazelcastJsonValueBigIntegerVal extends ExpressionBiValue { public HazelcastJsonValue field1; public BigInteger field2; }
    public static class HazelcastJsonValueFloatVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Float field2; }
    public static class HazelcastJsonValueDoubleVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Double field2; }
    public static class HazelcastJsonValueStringVal extends ExpressionBiValue { public HazelcastJsonValue field1; public String field2; }
    public static class HazelcastJsonValueCharacterVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Character field2; }
    public static class HazelcastJsonValueLocalDateVal extends ExpressionBiValue { public HazelcastJsonValue field1; public LocalDate field2; }
    public static class HazelcastJsonValueLocalTimeVal extends ExpressionBiValue { public HazelcastJsonValue field1; public LocalTime field2; }
    public static class HazelcastJsonValueLocalDateTimeVal extends ExpressionBiValue { public HazelcastJsonValue field1; public LocalDateTime field2; }
    public static class HazelcastJsonValueOffsetDateTimeVal extends ExpressionBiValue { public HazelcastJsonValue field1; public OffsetDateTime field2; }
    public static class HazelcastJsonValueObjectVal extends ExpressionBiValue { public HazelcastJsonValue field1; public Object field2; }

    public static class HazelcastJsonValueHazelcastJsonValueVal extends ExpressionBiValue { public HazelcastJsonValue field1; public HazelcastJsonValue field2; }
}
