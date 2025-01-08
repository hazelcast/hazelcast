/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.properties;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PropertyTypeConverterTest {

    @Test(expected = NullPointerException.class)
    public void test_string_converter_thenNullPointerException() {
        PropertyTypeConverter.STRING.convert(null);
    }

    @Test
    public void test_string_converter() {
        assertEquals("test", PropertyTypeConverter.STRING.convert("test"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_short_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.SHORT.convert(null);
    }

    @Test(expected = NumberFormatException.class)
    public void test_short_converter_thenNumberFormatException() {
        PropertyTypeConverter.SHORT.convert("test");
    }

    @Test
    public void test_short_converter() {
        assertEquals(Short.MAX_VALUE, PropertyTypeConverter.SHORT.convert(String.valueOf(Short.MAX_VALUE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_int_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.INTEGER.convert(null);
    }

    @Test(expected = NumberFormatException.class)
    public void test_int_converter_thenNumberFormatException() {
        PropertyTypeConverter.INTEGER.convert("test");
    }

    @Test
    public void test_int_converter() {
        assertEquals(Integer.MAX_VALUE, PropertyTypeConverter.INTEGER.convert(String.valueOf(Integer.MAX_VALUE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_long_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.LONG.convert(null);
    }

    @Test(expected = NumberFormatException.class)
    public void test_long_converter_thenNumberFormatException() {
        PropertyTypeConverter.LONG.convert("test");
    }

    @Test
    public void test_long_converter() {
        assertEquals(Long.MAX_VALUE, PropertyTypeConverter.LONG.convert(String.valueOf(Long.MAX_VALUE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_float_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.FLOAT.convert(null);
    }

    @Test(expected = NumberFormatException.class)
    public void test_float_converter_thenNumberFormatException() {
        PropertyTypeConverter.FLOAT.convert("test");
    }

    @Test
    public void test_float_converter() {
        assertEquals(Float.MAX_VALUE, PropertyTypeConverter.FLOAT.convert(String.valueOf(Float.MAX_VALUE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_double_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.DOUBLE.convert(null);
    }

    @Test(expected = NumberFormatException.class)
    public void test_double_converter_thenNumberFormatException() {
        PropertyTypeConverter.DOUBLE.convert("test");
    }

    @Test
    public void test_double_converter() {
        assertEquals(Double.MAX_VALUE, PropertyTypeConverter.DOUBLE.convert(String.valueOf(Double.MAX_VALUE)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_boolean_converter_thenIllegalArgumentException() {
        PropertyTypeConverter.BOOLEAN.convert(null);
    }

    @Test
    public void test_boolean_converter_false() {
        assertFalse((Boolean) PropertyTypeConverter.BOOLEAN.convert("test"));
    }

    @Test
    public void test_boolean_converter_true() {
        assertTrue((Boolean) PropertyTypeConverter.BOOLEAN.convert("true"));
    }
}
