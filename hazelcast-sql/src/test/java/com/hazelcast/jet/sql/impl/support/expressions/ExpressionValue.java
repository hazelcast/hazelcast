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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"unused", "unchecked, checkstyle:MultipleVariableDeclarations"})
public abstract class ExpressionValue implements DataSerializable {

    private static final ConcurrentHashMap<String, Class<? extends ExpressionValue>> CLASS_CACHE = new ConcurrentHashMap<>();

    public int key;

    public static Class<? extends ExpressionValue> createClass(ExpressionType<?> type) {
        return createClass(type.typeName());
    }

    public static Class<? extends ExpressionValue> createClass(String type) {
        return CLASS_CACHE.computeIfAbsent(type, (k) -> createClass0(type));
    }

    public static Class<? extends ExpressionValue> createClass0(String type) {
        try {
            String className = ExpressionValue.class.getName() + "$" + type + "Val";

            return (Class<? extends ExpressionValue>) Class.forName(className);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot create " + ExpressionValue.class.getSimpleName() + " for type \""
                    + type + "\"", e);
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public static String classForType(QueryDataTypeFamily type) {
        Class<?> valueClass = Converters.getConverters().stream()
                .filter(c -> c.getTypeFamily() == type)
                .findAny()
                .get()
                .getNormalizedValueClass();

        return ExpressionValue.class.getName() + "$" + valueClass.getSimpleName() + "Val";
    }

    public static <T extends ExpressionValue> T create(String className) {
        try {
            Class<? extends ExpressionValue> clazz = (Class<? extends ExpressionValue>) Class.forName(className);

            return create(clazz);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot create " + ExpressionValue.class.getSimpleName() + " for class \""
                    + className + "\"", e);
        }
    }

    public static <T extends ExpressionValue> T create(Class<? extends ExpressionValue> clazz) {
        try {
            return (T) clazz.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to create an instance of " + clazz.getSimpleName());
        }
    }

    public static <T extends ExpressionValue> T create(Class<? extends ExpressionValue> clazz, Object field) {
        return create(clazz, 0, field);
    }

    public static <T extends ExpressionValue> T create(Class<? extends ExpressionValue> clazz, int key, Object field) {
        T res = create(clazz);

        res.key = key;
        res.field1(field);

        return res;
    }

    public Object field1() {
        return getField("field1");
    }

    public ExpressionValue field1(Object value) {
        setField("field1", value);

        return this;
    }

    protected Object getField(String name) {
        try {
            return getClass().getField(name).get(this);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    protected void setField(String name, Object value) {
        try {
            getClass().getField(name).set(this, value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "[" + field1() + "]";
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(key);
        out.writeObject(field1());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readInt();
        field1(in.readObject());
    }

    public static class BooleanVal extends ExpressionValue { public Boolean field1; }
    public static class ByteVal extends ExpressionValue { public Byte field1; }
    public static class ShortVal extends ExpressionValue { public Short field1; }
    public static class IntegerVal extends ExpressionValue { public Integer field1; }
    public static class LongVal extends ExpressionValue { public Long field1; }
    public static class BigDecimalVal extends ExpressionValue { public BigDecimal field1; }
    public static class BigIntegerVal extends ExpressionValue { public BigInteger field1; }
    public static class FloatVal extends ExpressionValue { public Float field1; }
    public static class DoubleVal extends ExpressionValue { public Double field1; }
    public static class StringVal extends ExpressionValue { public String field1; }
    public static class CharacterVal extends ExpressionValue { public Character field1; }
    public static class LocalDateVal extends ExpressionValue { public LocalDate field1; }
    public static class LocalTimeVal extends ExpressionValue { public LocalTime field1; }
    public static class LocalDateTimeVal extends ExpressionValue { public LocalDateTime field1; }
    public static class OffsetDateTimeVal extends ExpressionValue { public OffsetDateTime field1; }
    public static class HazelcastJsonValueVal extends ExpressionValue { public HazelcastJsonValue field1; }

    public static class ObjectVal extends ExpressionValue {
        public Object field1;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ObjectVal objectVal = (ObjectVal) o;

            return Objects.equals(field1, objectVal.field1);
        }

        @Override
        public int hashCode() {
            return field1 != null ? field1.hashCode() : 0;
        }
    }
}
