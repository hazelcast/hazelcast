/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkHasNext;
import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

//@RunWith(HazelcastParallelClassRunner.class)
//@Category({QuickTest.class, ParallelJVMTest.class})
public class PreconditionsTest {


    // =====================================================

    @Test
    public void checkNotNull_whenNull() {
        String msg = "Can't be null";

        try {
            Preconditions.checkNotNull(null, msg);
            fail();
        } catch (NullPointerException expected) {
            assertEquals(msg, expected.getMessage());
        }
    }

    @Test
    public void checkNotNull_whenNotNull() {
        Object o = "foobar";

        Object result = Preconditions.checkNotNull(o, "");

        assertSame(o, result);
    }

    // =====================================================

    @Test
    public void checkNegative_long() {
        checkNegative_long(-1, true);
        checkNegative_long(0, false);
        checkNegative_long(1, false);
    }

    public void checkNegative_long(long value, boolean success) {
        String msg = "somemessage";

        if (success) {
            long result = Preconditions.checkNegative(value, msg);
            Assert.assertEquals(result, value);
        } else {
            try {
                Preconditions.checkNegative(value, msg);
            } catch (IllegalArgumentException expected) {
                assertSame(msg, expected.getMessage());
            }
        }
    }

    @Test
    public void checkNotNegative_long() {
        checkNotNegative_long(-1, false);
        checkNotNegative_long(0, true);
        checkNotNegative_long(1, true);
    }

    public void checkNotNegative_long(long value, boolean success) {
        String msg = "somemessage";

        if (success) {
            long result = Preconditions.checkNotNegative(value, msg);
            Assert.assertEquals(result, value);
        } else {
            try {
                Preconditions.checkNotNegative(value, msg);
            } catch (IllegalArgumentException expected) {
                assertSame(msg, expected.getMessage());
            }
        }
    }

    @Test
    public void checkNotNegative_int() {
        checkNotNegative_int(-1, false);
        checkNotNegative_int(0, true);
        checkNotNegative_int(1, true);
    }

    public void checkNotNegative_int(int value, boolean success) {
        String msg = "somemessage";

        if (success) {
            long result = Preconditions.checkNotNegative(value, msg);
            Assert.assertEquals(result, value);
        } else {
            try {
                Preconditions.checkNotNegative(value, msg);
            } catch (IllegalArgumentException expected) {
                assertSame(msg, expected.getMessage());
            }
        }
    }

    @Test
    public void checkPositive_long() {
        checkPositive_long(-1, false);
        checkPositive_long(0, false);
        checkPositive_long(1, true);
    }

    public void checkPositive_long(long value, boolean success) {
        String paramName = "someParamName";

        if (success) {
            long result = Preconditions.checkPositive(paramName, value);
            Assert.assertEquals(result, value);
        } else {
            try {
                Preconditions.checkPositive(paramName, value);
                fail();
            } catch (IllegalArgumentException expected) {
                assertEquals(paramName + " is " + value + " but must be > 0", expected.getMessage());
            }
        }
    }

    @Test
    public void checkPositive_int() {
        checkPositive_int(-1, false);
        checkPositive_int(0, false);
        checkPositive_int(1, true);
    }

    public void checkPositive_int(int value, boolean success) {
        String msg = "somemessage";

        if (success) {
            long result = Preconditions.checkPositive(msg, value);
            Assert.assertEquals(result, value);
        } else {
            try {
                Preconditions.checkPositive(value, msg);
                fail();
            } catch (IllegalArgumentException expected) {
                assertSame(msg, expected.getMessage());
            }
        }
    }

    @Test
    public void checkHasText() {
        checkHasText(null, false);
        checkHasText("", false);
        checkHasText("foobar", true);
    }

    public void checkHasText(String value, boolean success) {
        String msg = "somemessage";

        if (success) {
            String result = Preconditions.checkHasText(value, msg);
            assertSame(result, value);
        } else {
            try {
                Preconditions.checkHasText(value, msg);
                fail();
            } catch (IllegalArgumentException expected) {
                assertSame(msg, expected.getMessage());
            }
        }
    }

    @Test
    public void test_checkInstanceOf() throws Exception {
        Number value = checkInstanceOf(Number.class, Integer.MAX_VALUE, "argumentName");
        assertEquals("Returned value should be " + Integer.MAX_VALUE, Integer.MAX_VALUE, value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkInstanceOf_whenSuppliedObjectIsNotInstanceOfExpectedType() throws Exception {
        checkInstanceOf(Integer.class, BigInteger.ONE, "argumentName");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkInstanceOf_withNullType() throws Exception {
        checkInstanceOf(null, Integer.MAX_VALUE, "argumentName");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkInstanceOf_withNullObject() throws Exception {
        checkInstanceOf(Number.class, null, "argumentName");
    }

    @Test
    public void test_checkNotInstanceOf() throws Exception {
        BigInteger value = checkNotInstanceOf(Integer.class, BigInteger.ONE, "argumentName");
        assertEquals("Returned value should be equal to BigInteger.ONE", BigInteger.ONE, value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkNotInstanceOf_whenSuppliedObjectIsInstanceOfExpectedType() throws Exception {
        checkNotInstanceOf(Integer.class, Integer.MAX_VALUE, "argumentName");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkNotInstanceOf_withNullType() throws Exception {
        checkNotInstanceOf(null, BigInteger.ONE, "argumentName");
    }

    @Test
    public void test_checkNotInstanceOf_withNullObject() throws Exception {
        Object value = checkNotInstanceOf(Integer.class, null, "argumentName");
        assertNull(value);
    }

    @Test
    public void test_checkFalse_whenFalse() throws Exception {
        checkFalse(false, "comparison cannot be true");
    }

    @Test
    public void test_checkFalse_whenTrue() throws Exception {
        String errorMessage = "foobar";
        try {
            checkFalse(true, errorMessage);
            fail();
        } catch (IllegalArgumentException e) {
            assertSame(errorMessage, e.getMessage());
        }
    }

    @Test
    public void test_checkTrue_whenTrue() throws Exception {
        checkTrue(true, "must be true");
    }

    @Test
    public void test_checkTrue_whenFalse() throws Exception {
        String errorMessage = "foobar";
        try {
            checkTrue(false, errorMessage);
            fail();
        } catch (IllegalArgumentException e) {
            assertSame(errorMessage, e.getMessage());
        }
    }

    @Test
    public void test_checkFalse() throws Exception {
        try {
            checkFalse(Boolean.FALSE, "comparison cannot be true");
        } catch (Exception e) {
            fail("checkFalse method should not throw this exception " + e);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkFalse_whenComparisonTrue() throws Exception {
        checkFalse(Boolean.TRUE, "comparison cannot be true");
    }

    @Test(expected = NoSuchElementException.class)
    public void test_hasNextThrowsException_whenEmptyIteratorGiven() throws Exception {
        checkHasNext(Collections.emptyList().iterator(), "");
    }

    @Test
    public void test_hasNextReturnsIterator_whenNonEmptyIteratorGiven() throws Exception {
        Iterator<Integer> iterator = Arrays.asList(1, 2).iterator();
        assertEquals(iterator, checkHasNext(iterator, ""));
    }

    @Test
    public void test_checkRequiredProperty_whenNoProperty() {
        Properties properties = new Properties();
        properties.setProperty("other-key", "other-value");
        Assertions.assertThatThrownBy(() -> checkRequiredProperty(properties, "some-key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'some-key' is required");
    }

    @Test
    public void test_checkRequiredProperty_whenProperty() {
        Properties properties = new Properties();
        properties.setProperty("some-key", "some-value");

        assertThat(checkRequiredProperty(properties, "some-key")).isEqualTo("some-value");
    }

    @Test
    public void test_checkRequiredProperty_when_null() {
        Assertions.assertThatThrownBy(() -> checkRequiredProperty(null, "some-key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Properties are required");
    }
}
