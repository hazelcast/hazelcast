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

package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static com.hazelcast.query.impl.TypeConverters.BOOLEAN_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.IDENTITY_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.LONG_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.NULL_CONVERTER;
import static com.hazelcast.query.impl.TypeConverters.STRING_CONVERTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeConverterTest {

    @Test
    public void testTransient() {
        assertTrue(converter(NULL_CONVERTER).isTransient());
        assertTrue(converter(INTEGER_CONVERTER, NULL_CONVERTER).isTransient());

        assertFalse(converter(INTEGER_CONVERTER).isTransient());
        assertFalse(converter(INTEGER_CONVERTER, IDENTITY_CONVERTER).isTransient());
    }

    @Test
    public void testComponentConverters() {
        assertSame(NULL_CONVERTER, converter(NULL_CONVERTER).getComponentConverter(0));
        assertSame(INTEGER_CONVERTER, converter(INTEGER_CONVERTER).getComponentConverter(0));

        assertSame(NULL_CONVERTER, converter(INTEGER_CONVERTER, NULL_CONVERTER).getComponentConverter(1));
        assertSame(INTEGER_CONVERTER, converter(INTEGER_CONVERTER, NULL_CONVERTER).getComponentConverter(0));

        assertSame(INTEGER_CONVERTER, converter(INTEGER_CONVERTER, NULL_CONVERTER, LONG_CONVERTER).getComponentConverter(0));
        assertSame(NULL_CONVERTER, converter(INTEGER_CONVERTER, NULL_CONVERTER, LONG_CONVERTER).getComponentConverter(1));
        assertSame(LONG_CONVERTER, converter(INTEGER_CONVERTER, NULL_CONVERTER, LONG_CONVERTER).getComponentConverter(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConversionAcceptsOnlyCompositeValues() {
        converter(STRING_CONVERTER).convert("value");
    }

    @Test
    public void testConversion() {
        assertEquals(value(1), converter(INTEGER_CONVERTER).convert(value(1)));
        assertEquals(value(1), converter(INTEGER_CONVERTER).convert(value("1")));

        assertEquals(value(1, true), converter(INTEGER_CONVERTER, BOOLEAN_CONVERTER).convert(value(1, true)));
        assertEquals(value(1, false), converter(INTEGER_CONVERTER, BOOLEAN_CONVERTER).convert(value(1.0, "non-true")));

        assertEquals(value(1, true, "foo"),
                converter(INTEGER_CONVERTER, BOOLEAN_CONVERTER, STRING_CONVERTER).convert(value(1, true, "foo")));
        assertEquals(value(1, false, "1"),
                converter(INTEGER_CONVERTER, BOOLEAN_CONVERTER, STRING_CONVERTER).convert(value(1.0, "non-true", 1)));
    }

    @Test
    public void testSpecialValuesArePreserved() {
        assertEquals(value(NULL), converter(INTEGER_CONVERTER).convert(value(NULL)));
        assertEquals(value(NEGATIVE_INFINITY), converter(INTEGER_CONVERTER).convert(value(NEGATIVE_INFINITY)));
        assertEquals(value(POSITIVE_INFINITY), converter(INTEGER_CONVERTER).convert(value(POSITIVE_INFINITY)));
    }

    private static CompositeConverter converter(TypeConverter... converters) {
        return new CompositeConverter(converters);
    }

    private static CompositeValue value(Comparable... values) {
        return new CompositeValue(values);
    }

}
