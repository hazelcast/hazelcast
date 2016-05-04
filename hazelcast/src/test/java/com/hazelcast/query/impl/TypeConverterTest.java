/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TypeConverterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testConvert_whenPassedNullValue_thenConvertToNullObject() {
        TypeConverters.BaseTypeConverter converter = new TypeConverters.BaseTypeConverter() {
            @Override
            Comparable convertInternal(Comparable value) {
                return value;
            }
        };
        assertEquals(IndexImpl.NULL, converter.convert(null));
    }

    @Test
    public void testBigIntegerConvert_whenPassedStringValue_thenConvertToBigInteger() throws Exception {
        String stringValue = "3141593";
        Comparable expectedBigIntValue = new BigInteger(stringValue);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(stringValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedDoubleValue_thenConvertToBigInteger() throws Exception {
        Double doubleValue = 3.141593;
        Comparable expectedBigIntValue = BigInteger.valueOf(doubleValue.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedFloatValue_thenConvertToBigInteger() throws Exception {
        Float doubleValue = 3.141593F;
        Comparable expectedBigIntValue = BigInteger.valueOf(3);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedLongValue_thenConvertToBigInteger() throws Exception {
        Long longValue = 3141593L;
        Comparable expectedBigIntValue = BigInteger.valueOf(longValue.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(longValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedIntegerValue_thenConvertToBigInteger() throws Exception {
        Integer integerValue = 3141593;
        Comparable expectedBigIntValue = BigInteger.valueOf(integerValue.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(integerValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedBigDecimalValue_thenConvertToBigInteger() throws Exception {
        BigDecimal value = BigDecimal.valueOf(4.9999);
        Comparable expectedBigIntValue = BigInteger.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedHugeBigDecimalValue_thenConvertToBigInteger() throws Exception {
        BigDecimal value = BigDecimal.ONE.add(
                BigDecimal.valueOf(Long.MAX_VALUE));
        Comparable expectedBigIntValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedBigIntegerValue_thenConvertToBigInteger() throws Exception {
        BigInteger value = BigInteger.ONE;
        Comparable expectedBigIntValue = BigInteger.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedBooleanValue_thenConvertToBigInteger() throws Exception {
        Boolean value = Boolean.TRUE;
        Comparable trueAsNumber = BigInteger.ONE; // Boolean TRUE means non-zero value, i.e. 1, FALSE means 0

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(trueAsNumber))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedNullValue_thenConvertToBigInteger() throws Exception {
        Comparable value = "NotANumber";
        thrown.expect(NumberFormatException.class);
        thrown.expectMessage(startsWith("For input string: "));

        TypeConverters.BIG_INTEGER_CONVERTER.convert(value);
    }

    @Test
    public void testBigDecimalConvert_whenPassedStringValue_thenConvertToBigDecimal() throws Exception {
        String stringValue = "3141593";
        Comparable expectedDecimal = new BigDecimal(stringValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(stringValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedDoubleValue_thenConvertToBigDecimal() throws Exception {
        Double doubleValue = 3.141593;
        Comparable expectedDecimal = new BigDecimal(doubleValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedFloatValue_thenConvertToBigDecimal() throws Exception {
        Float floatValue = 3.141593F;
        Comparable expectedDecimal = new BigDecimal(floatValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(floatValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedLongValue_thenConvertToBigDecimal() throws Exception {
        Long longValue = 3141593L;
        Comparable expectedDecimal = BigDecimal.valueOf(longValue.longValue());

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(longValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedIntegerValue_thenConvertToBigDecimal() throws Exception {
        Integer integerValue = 3141593;
        Comparable expectedDecimal = new BigDecimal(integerValue.toString());

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(integerValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedHugeBigIntegerValue_thenConvertToBigDecimal() throws Exception {
        BigInteger value = BigInteger.ONE.add(
                BigInteger.valueOf(Long.MAX_VALUE));
        Comparable expectedDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedBigIntegerValue_thenConvertToBigDecimal() throws Exception {
        BigInteger value = BigInteger.ONE;
        Comparable expectedDecimal = BigDecimal.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedBooleanValue_thenConvertToBigDecimal() throws Exception {
        Boolean value = Boolean.TRUE;
        Comparable trueAsDecimal = BigDecimal.ONE; // Boolean TRUE means non-zero value, i.e. 1, FALSE means 0

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(trueAsDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedNullValue_thenConvertToBigDecimal() throws Exception {
        Comparable value = "NotANumber";
        thrown.expect(NumberFormatException.class);

        TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);
    }
}
