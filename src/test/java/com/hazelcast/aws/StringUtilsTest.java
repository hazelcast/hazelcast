package com.hazelcast.aws;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StringUtilsTest {

    @Test
    public void isEmpty() {
        assertTrue(StringUtils.isEmpty(null));
        assertTrue(StringUtils.isEmpty(""));
        assertTrue(StringUtils.isEmpty("   \t   "));
    }

    @Test
    public void isNotEmpty() {
        assertTrue(StringUtils.isNotEmpty("some-string"));
    }
}