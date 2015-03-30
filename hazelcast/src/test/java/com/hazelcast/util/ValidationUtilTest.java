package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.util.ValidationUtil.checkFalse;
import static com.hazelcast.util.ValidationUtil.checkInstanceOf;
import static com.hazelcast.util.ValidationUtil.checkNotInstanceOf;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ValidationUtilTest {

    @Test
    public void test_checkInstanceOf() throws Exception {
        int value = checkInstanceOf(Number.class, Integer.MAX_VALUE, "argumentName");
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
        ValidationUtil.checkHasNext(Collections.emptyList().iterator(), "");
    }

    @Test
    public void test_hasNextReturnsIterator_whenNonEmptyIteratorGiven() throws Exception {
        Iterator<Integer> iterator = Arrays.asList(1, 2).iterator();
        assertEquals(iterator, ValidationUtil.checkHasNext(iterator, ""));
    }
}