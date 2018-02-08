/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import static org.hamcrest.CoreMatchers.not;
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
    public void testBigIntegerConvert_whenPassedStringValue_thenConvertToBigInteger() {
        String stringValue = "3141593";
        Comparable expectedBigIntValue = new BigInteger(stringValue);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(stringValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedDoubleValue_thenConvertToBigInteger() {
        Double doubleValue = 3.141593;
        Comparable expectedBigIntValue = BigInteger.valueOf(doubleValue.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedFloatValue_thenConvertToBigInteger() {
        Float doubleValue = 3.141593F;
        Comparable expectedBigIntValue = BigInteger.valueOf(3);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedLongValue_thenConvertToBigInteger() {
        Long longValue = 3141593L;
        Comparable expectedBigIntValue = BigInteger.valueOf(longValue);

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(longValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedIntegerValue_thenConvertToBigInteger() {
        Integer integerValue = 3141593;
        Comparable expectedBigIntValue = BigInteger.valueOf(integerValue.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(integerValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedBigDecimalValue_thenConvertToBigInteger() {
        BigDecimal value = BigDecimal.valueOf(4.9999);
        Comparable expectedBigIntValue = BigInteger.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedHugeBigDecimalValue_thenConvertToBigInteger() {
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
    public void testBigIntegerConvert_whenPassedBigIntegerValue_thenConvertToBigInteger() {
        BigInteger value = BigInteger.ONE;
        Comparable expectedBigIntValue = BigInteger.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(expectedBigIntValue))
        ));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testBigIntegerConvert_whenPassedBooleanValue_thenConvertToBigInteger() {
        // Boolean TRUE means non-zero value, i.e. 1, FALSE means 0
        Boolean value = Boolean.TRUE;
        Comparable trueAsNumber = BigInteger.ONE;

        Comparable comparable = TypeConverters.BIG_INTEGER_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigInteger.class)),
                is(equalTo(trueAsNumber))
        ));
    }

    @Test
    public void testBigIntegerConvert_whenPassedNullValue_thenConvertToBigInteger() {
        Comparable value = "NotANumber";
        thrown.expect(NumberFormatException.class);
        thrown.expectMessage(startsWith("For input string: "));

        TypeConverters.BIG_INTEGER_CONVERTER.convert(value);
    }

    @Test
    public void testBigDecimalConvert_whenPassedStringValue_thenConvertToBigDecimal() {
        String stringValue = "3141593";
        Comparable expectedDecimal = new BigDecimal(stringValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(stringValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    /**
     * Checks that the {@link TypeConverters#BIG_DECIMAL_CONVERTER} doesn't return a rounded {@link BigDecimal}.
     */
    @Test
    public void testBigDecimalConvert_whenPassedDoubleValue_thenConvertToBigDecimal() {
        Double doubleValue = 3.141593;
        Comparable expectedDecimal = BigDecimal.valueOf(doubleValue);
        Comparable unexpectedDecimal = new BigDecimal(doubleValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(doubleValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal)),
                not(equalTo(unexpectedDecimal))
        ));
    }

    /**
     * Checks that the {@link TypeConverters#BIG_DECIMAL_CONVERTER} doesn't return a rounded {@link BigDecimal}.
     */
    @Test
    public void testBigDecimalConvert_whenPassedFloatValue_thenConvertToBigDecimal() {
        Float floatValue = 3.141593F;
        Comparable expectedDecimal = BigDecimal.valueOf(floatValue);
        Comparable unexpectedDecimal = new BigDecimal(floatValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(floatValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal)),
                not(equalTo(unexpectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedLongValue_thenConvertToBigDecimal() {
        Long longValue = 3141593L;
        Comparable expectedDecimal = BigDecimal.valueOf(longValue);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(longValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedIntegerValue_thenConvertToBigDecimal() {
        Integer integerValue = 3141593;
        Comparable expectedDecimal = new BigDecimal(integerValue.toString());

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(integerValue);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedHugeBigIntegerValue_thenConvertToBigDecimal() {
        BigInteger value = BigInteger.ONE.add(BigInteger.valueOf(Long.MAX_VALUE));
        Comparable expectedDecimal = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedBigIntegerValue_thenConvertToBigDecimal() {
        BigInteger value = BigInteger.ONE;
        Comparable expectedDecimal = BigDecimal.valueOf(value.longValue());

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(expectedDecimal))
        ));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testBigDecimalConvert_whenPassedBooleanValue_thenConvertToBigDecimal() {
        // Boolean TRUE means non-zero value, i.e. 1, FALSE means 0
        Boolean value = Boolean.TRUE;
        Comparable trueAsDecimal = BigDecimal.ONE;

        Comparable comparable = TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);

        assertThat(comparable, allOf(
                is(instanceOf(BigDecimal.class)),
                is(equalTo(trueAsDecimal))
        ));
    }

    @Test
    public void testBigDecimalConvert_whenPassedNullValue_thenConvertToBigDecimal() {
        Comparable value = "NotANumber";
        thrown.expect(NumberFormatException.class);

        TypeConverters.BIG_DECIMAL_CONVERTER.convert(value);
    }

    @Test
    public void testCharConvert_whenPassedNumeric_thenConvertToChar() {
        Comparable value = 1;
        Comparable expectedCharacter = (char) 1;

        Comparable actualCharacter = TypeConverters.CHAR_CONVERTER.convert(value);

        assertThat(actualCharacter, allOf(
                is(instanceOf(Character.class)),
                is(equalTo(expectedCharacter))
        ));
    }

    @Test
    public void testCharConvert_whenPassedString_thenConvertToChar() {
        Comparable value = "foo";
        Comparable expectedCharacter = 'f';

        Comparable actualCharacter = TypeConverters.CHAR_CONVERTER.convert(value);

        assertThat(actualCharacter, allOf(
                is(instanceOf(Character.class)),
                is(equalTo(expectedCharacter))
        ));
    }

    @Test
    public void testCharConvert_whenPassedEmptyString_thenConvertToChar() {
        Comparable value = "";
        thrown.expect(IllegalArgumentException.class);

        TypeConverters.CHAR_CONVERTER.convert(value);
    }
}
