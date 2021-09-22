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

package com.hazelcast.jet.sql.impl.expression.json;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class JsonPathUtilTest {
    @Test
    public void testByteFields() {
        final String json = jsonArray(1, 0, -1, Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertWithType(json, 0, Byte.class, (byte) 1);
        assertWithType(json, 1, Byte.class, (byte) 0);
        assertWithType(json, 2, Byte.class, (byte) -1);
        assertWithType(json, 3, Byte.class, Byte.MIN_VALUE);
        assertWithType(json, 4, Byte.class, Byte.MAX_VALUE);
    }

    @Test
    public void testShortFields() {
        final String json = jsonArray(1000, -1000, Short.MIN_VALUE, Short.MAX_VALUE);
        assertWithType(json, 0, Short.class, (short) 1000);
        assertWithType(json, 1, Short.class, (short) -1000);
        assertWithType(json, 2, Short.class, Short.MIN_VALUE);
        assertWithType(json, 3, Short.class, Short.MAX_VALUE);
    }

    @Test
    public void testIntegerFields() {
        final String json = jsonArray(100000, -100000, Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertWithType(json, 0, Integer.class, 100000);
        assertWithType(json, 1, Integer.class, -100000);
        assertWithType(json, 2, Integer.class, Integer.MIN_VALUE);
        assertWithType(json, 3, Integer.class, Integer.MAX_VALUE);
    }

    @Test
    public void testLongFields() {
        final String json = jsonArray(1_000_000_000_000L, -1_000_000_000_000L, Long.MIN_VALUE, Long.MAX_VALUE);
        assertWithType(json, 0, Long.class, 1_000_000_000_000L);
        assertWithType(json, 1, Long.class, -1_000_000_000_000L);
        assertWithType(json, 2, Long.class, Long.MIN_VALUE);
        assertWithType(json, 3, Long.class, Long.MAX_VALUE);
    }

    @Test
    public void testBigIntegerFields() {
        // TODO: 1e100 expressed in this way is interpreted by JsonPath/GSON as a string, potentially an issue
        final BigInteger positive = BigInteger.TEN.pow(50);
        final BigInteger negative = positive.negate();
        final String json = jsonArray(positive, negative);
        assertWithType(json, 0, BigInteger.class, positive);
        assertWithType(json, 1, BigInteger.class, negative);
    }

    @Test
    public void testFloatFields() {
        final String json = jsonArray(1.0f, -1.0f, 0.0f, "1e100", "-1e100", "1e-100", "-1e-100");
        assertThat(jsonElementAt(json, 0))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.0d, offset(0.01d));
        assertThat(jsonElementAt(json, 1))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(-1.0d, offset(0.01d));
        assertThat(jsonElementAt(json, 2))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(0.0d, offset(0.01d));
        assertThat(jsonElementAt(json, 3))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.0e100, offset(1e98));
        assertThat(jsonElementAt(json, 4))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(-1.0e100, offset(1e98));
        assertThat(jsonElementAt(json, 5))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.0e-100, offset(1e-102));
        assertThat(jsonElementAt(json, 6))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(-1.0e-100, offset(1e-102));
    }

    @Test
    public void testDoubleFields() {
        final String json = jsonArray("1e308", "-1e308", "1e-306", "-1e-306");
        assertThat(jsonElementAt(json, 0))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.0e308, offset(1.0e306));
        assertThat(jsonElementAt(json, 1))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(-1.0e308, offset(1.0e306));
        assertThat(jsonElementAt(json, 2))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.0e-306, offset(1e-308));
        assertThat(jsonElementAt(json, 3))
                .isInstanceOf(Double.class)
                .asInstanceOf(InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(-1.0e-306, offset(1e-308));
    }

    @Test
    public void testBigDecimalFields() {
        // TODO: numbers like 1e-500, -1e-500 are returned as 0.0
        final String json = jsonArray("1e500", "-1e500");
        assertThat(jsonElementAt(json, 0))
                .isInstanceOf(BigDecimal.class)
                .asInstanceOf(InstanceOfAssertFactories.BIG_DECIMAL)
                .isEqualTo(new BigDecimal("1e500"));
        assertThat(jsonElementAt(json, 1))
                .isInstanceOf(BigDecimal.class)
                .asInstanceOf(InstanceOfAssertFactories.BIG_DECIMAL)
                .isEqualTo(new BigDecimal("-1e500"));
    }

    @Test
    public void testBooleanFields() {
        final String json = jsonArray("true", "false");
        assertWithType(json, 0, Boolean.class, true);
        assertWithType(json, 1, Boolean.class, false);
    }

    @Test
    public void testVarcharFields() {
        final String json = jsonArray("\"test\"", "\"value\"");
        assertWithType(json, 0, String.class, "test");
        assertWithType(json, 1, String.class, "value");
    }

    private Object jsonElementAt(String json, int index) {
        return JsonPathUtil.read(json, format("$[%d]", index));
    }

    private void assertWithType(String json, int index, Class<?> type, Object value) {
        assertThat(jsonElementAt(json, index)).isInstanceOf(type).isEqualTo(value);
    }

    private String jsonArray(Object ...values) {
        return format("[%s]", Stream.of(values)
                .map(Object::toString)
                .collect(Collectors.joining(",")));
    }
}
