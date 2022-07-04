/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DATE;
import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.FLOAT;
import static com.hazelcast.nio.serialization.FieldType.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.INT;
import static com.hazelcast.nio.serialization.FieldType.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.LONG;
import static com.hazelcast.nio.serialization.FieldType.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.PORTABLE;
import static com.hazelcast.nio.serialization.FieldType.PORTABLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.SHORT;
import static com.hazelcast.nio.serialization.FieldType.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UTF;
import static com.hazelcast.nio.serialization.FieldType.UTF_ARRAY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class FieldTypeTest {

    @Test
    public void correctNonArrayTypes() {
        assertFalse(BYTE.isArrayType());
        assertFalse(BOOLEAN.isArrayType());
        assertFalse(CHAR.isArrayType());
        assertFalse(SHORT.isArrayType());
        assertFalse(INT.isArrayType());
        assertFalse(LONG.isArrayType());
        assertFalse(FLOAT.isArrayType());
        assertFalse(DOUBLE.isArrayType());
        assertFalse(UTF.isArrayType());
        assertFalse(BYTE.isArrayType());
        assertFalse(DECIMAL.isArrayType());
        assertFalse(TIME.isArrayType());
        assertFalse(DATE.isArrayType());
        assertFalse(TIMESTAMP.isArrayType());
        assertFalse(TIMESTAMP_WITH_TIMEZONE.isArrayType());
    }

    @Test
    public void correctArrayTypes() {
        assertTrue(PORTABLE_ARRAY.isArrayType());
        assertTrue(BYTE_ARRAY.isArrayType());
        assertTrue(BOOLEAN_ARRAY.isArrayType());
        assertTrue(CHAR_ARRAY.isArrayType());
        assertTrue(SHORT_ARRAY.isArrayType());
        assertTrue(INT_ARRAY.isArrayType());
        assertTrue(LONG_ARRAY.isArrayType());
        assertTrue(FLOAT_ARRAY.isArrayType());
        assertTrue(DOUBLE_ARRAY.isArrayType());
        assertTrue(UTF_ARRAY.isArrayType());
        assertTrue(DECIMAL_ARRAY.isArrayType());
        assertTrue(TIME_ARRAY.isArrayType());
        assertTrue(DATE_ARRAY.isArrayType());
        assertTrue(TIMESTAMP_ARRAY.isArrayType());
        assertTrue(TIMESTAMP_WITH_TIMEZONE_ARRAY.isArrayType());
    }

    @Test
    public void correctSingleTypesConversion() {
        assertEquals(PORTABLE, PORTABLE_ARRAY.getSingleType());
        assertEquals(BYTE, BYTE_ARRAY.getSingleType());
        assertEquals(BOOLEAN, BOOLEAN_ARRAY.getSingleType());
        assertEquals(CHAR, CHAR_ARRAY.getSingleType());
        assertEquals(SHORT, SHORT_ARRAY.getSingleType());
        assertEquals(INT, INT_ARRAY.getSingleType());
        assertEquals(LONG, LONG_ARRAY.getSingleType());
        assertEquals(FLOAT, FLOAT_ARRAY.getSingleType());
        assertEquals(DOUBLE, DOUBLE_ARRAY.getSingleType());
        assertEquals(DECIMAL, DECIMAL_ARRAY.getSingleType());
        assertEquals(TIME, TIME_ARRAY.getSingleType());
        assertEquals(DATE, DATE_ARRAY.getSingleType());
        assertEquals(TIMESTAMP, TIMESTAMP_ARRAY.getSingleType());
        assertEquals(TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE_ARRAY.getSingleType());
    }

}
