package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;

import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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

        if(success){
            long result = Preconditions.checkNegative(value,msg);
            Assert.assertEquals(result, value);
        }else{
            try {
                Preconditions.checkNegative(value, msg);
            }catch (IllegalArgumentException expected){
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

        if(success){
            long result = Preconditions.checkNotNegative(value, msg);
            Assert.assertEquals(result, value);
        }else{
            try {
                Preconditions.checkNotNegative(value, msg);
            }catch (IllegalArgumentException expected){
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

        if(success){
            long result = Preconditions.checkNotNegative(value, msg);
            Assert.assertEquals(result, value);
        }else{
            try {
                Preconditions.checkNotNegative(value, msg);
            }catch (IllegalArgumentException expected){
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
        String msg = "somemessage";

        if(success){
            long result = Preconditions.checkPositive(value, msg);
            Assert.assertEquals(result, value);
        }else{
            try {
                Preconditions.checkPositive(value, msg);
            }catch (IllegalArgumentException expected){
                assertSame(msg, expected.getMessage());
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

        if(success){
            long result = Preconditions.checkPositive(value, msg);
            Assert.assertEquals(result, value);
        }else{
            try {
                Preconditions.checkPositive(value, msg);
            }catch (IllegalArgumentException expected){
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

        if(success){
            String result = Preconditions.checkHasText(value, msg);
            assertSame(result, value);
        }else{
            try {
                Preconditions.checkHasText(value, msg);
            }catch (IllegalArgumentException expected){
                assertSame(msg, expected.getMessage());
            }
        }
    }

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
}