package com.hazelcast.test;

import org.junit.Test;

import static org.junit.Assert.*;

public class HazelcastTestSupportTest {

    @Test(expected = AssertionError.class)
    public void testAssertLesser_failWhenEquals() throws Exception {
        HazelcastTestSupport.assertLesser(0, 0);
    }

    @Test(expected = AssertionError.class)
    public void testAssertLesser_failWhenGreaterThanLimit() throws Exception {
        HazelcastTestSupport.assertLesser(0, 1);
    }

    @Test
    public void testAssertLesser_passWhenLesser() throws Exception {
        HazelcastTestSupport.assertLesser(1, 0);
    }


    @Test(expected = AssertionError.class)
    public void assertLesserOrEquals_failWhenGreaterThanLimit() {
        HazelcastTestSupport.assertLesserOrEquals(0, 1);
    }

    @Test
    public void assertLesserOrEquals_passWhenEquals() {
        HazelcastTestSupport.assertLesserOrEquals(0, 0);
    }

    @Test
    public void assertLesserOrEquals_passLesser() {
        HazelcastTestSupport.assertLesserOrEquals(0, 0);
    }
}