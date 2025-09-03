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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Locale;

import static com.hazelcast.internal.util.StringUtil.VERSION_PATTERN;
import static com.hazelcast.internal.util.StringUtil.isAllNullOrEmptyAfterTrim;
import static com.hazelcast.internal.util.StringUtil.isAnyNullOrEmptyAfterTrim;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
class StringUtilTest extends HazelcastTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"3.1", "3.1-SNAPSHOT", "3.1-RC", "3.1-RC1-SNAPSHOT", "3.1.1", "3.1.1-RC", "3.1.1-SNAPSHOT",
            "3.1.1-RC1-SNAPSHOT"})
    void testVersionPatternMatches(String input) {
        assertTrue(VERSION_PATTERN.matcher(input).matches());
    }

    @ParameterizedTest
    @ValueSource(strings = {"${project.version}", "project.version", "3", "3.RC", "3.SNAPSHOT", "3-RC", "3-SNAPSHOT", "3.",
            "3.1.RC", "3.1.SNAPSHOT"})
    void testVersionPatternNotMatches(String input) {
        assertFalse(VERSION_PATTERN.matcher(input).matches());
    }


    @Test
    void testSplitByComma() {
        assertNull(StringUtil.splitByComma(null, true));
        assertArrayEquals(arr(""), StringUtil.splitByComma("", true));
        assertArrayEquals(arr(""), StringUtil.splitByComma(" ", true));
        assertArrayEquals(arr(), StringUtil.splitByComma(" ", false));
        assertArrayEquals(arr("a"), StringUtil.splitByComma("a", true));
        assertArrayEquals(arr("a"), StringUtil.splitByComma("a", false));
        assertArrayEquals(arr("aa", "bbb", "c"), StringUtil.splitByComma("aa,bbb,c", true));
        assertArrayEquals(arr("aa", "bbb", "c", ""), StringUtil.splitByComma(" aa\t,\nbbb   ,\r c,  ", true));
        assertArrayEquals(arr("aa", "bbb", "c"), StringUtil.splitByComma("  aa ,\n,\r\tbbb  ,c , ", false));
    }

    @Test
    void testArrayIntersection() {
        assertArrayEquals(arr("test"), StringUtil.intersection(arr("x", "test", "y", "z"), arr("a", "b", "test")));
        assertArrayEquals(arr(""), StringUtil.intersection(arr("", "z"), arr("a", "")));
        assertArrayEquals(arr(), StringUtil.intersection(arr("", "z"), arr("a")));
    }

    @Test
    void testArraySubtraction() {
        assertNull(StringUtil.subtraction(null, arr("a", "test", "b", "a")));
        assertArrayEquals(arr("a", "test", "b", "a"), StringUtil.subtraction(arr("a", "test", "b", "a"), null));
        assertArrayEquals(arr("test"), StringUtil.subtraction(arr("a", "test", "b", "a"), arr("a", "b")));
        assertArrayEquals(arr(), StringUtil.subtraction(arr(), arr("a", "b")));
        assertArrayEquals(arr("a", "b"), StringUtil.subtraction(arr("a", "b"), arr()));
        assertArrayEquals(arr(), StringUtil.subtraction(arr("a", "test", "b", "a"), arr("a", "b", "test")));
    }

    @Test
    void testEqualsIgnoreCase() {
        assertFalse(StringUtil.equalsIgnoreCase(null, null));
        assertFalse(StringUtil.equalsIgnoreCase(null, "a"));
        assertFalse(StringUtil.equalsIgnoreCase("a", null));
        assertTrue(StringUtil.equalsIgnoreCase("TEST", "test"));
        assertTrue(StringUtil.equalsIgnoreCase("test", "TEST"));
        assertFalse(StringUtil.equalsIgnoreCase("test", "TEST2"));

        Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(new Locale("tr"));
        try {
            assertTrue(StringUtil.equalsIgnoreCase("EXIT", "exit"));
            assertFalse(StringUtil.equalsIgnoreCase("exıt", "EXIT"));
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    void testStripTrailingSlash() {
        assertNull(StringUtil.stripTrailingSlash(null));
        assertEquals("", StringUtil.stripTrailingSlash(""));
        assertEquals("a", StringUtil.stripTrailingSlash("a"));
        assertEquals("a", StringUtil.stripTrailingSlash("a/"));
        assertEquals("a/a", StringUtil.stripTrailingSlash("a/a"));
        assertEquals("a/", StringUtil.stripTrailingSlash("a//"));
    }

    @Test
    void testResolvePlaceholders() {
        assertResolvePlaceholder(
                "noPlaceholders",
                "noPlaceholders",
                "", "param", "value");
        assertResolvePlaceholder(
                "param: value",
                "param: ${param}",
                "", "param", "value");
        assertResolvePlaceholder(
                "param: ${param",
                "param: ${param",
                "", "param", "value");
        assertResolvePlaceholder(
                "missing: ${missing}",
                "missing: ${missing}",
                "", "param", "value");
        assertResolvePlaceholder(
                "missing: ${missing}, param: value",
                "missing: ${missing}, param: ${param}",
                "", "param", "value");
        assertResolvePlaceholder(
                "broken: ${broken, param: ${param}",
                "broken: ${broken, param: ${param}",
                "", "param", "value");
        assertResolvePlaceholder(
                "param: value, broken: ${broken",
                "param: ${param}, broken: ${broken",
                "", "param", "value");
        assertResolvePlaceholder(
                "missing: ${missing}, param: value, broken: ${broken",
                "missing: ${missing}, param: ${param}, broken: ${broken",
                "", "param", "value");
        assertResolvePlaceholder(
                "param1: value1, param2: value2, param3: value3",
                "param1: ${param1}, param2: ${param2}, param3: ${param3}",
                "", "param1", "value1", "param2", "value2", "param3", "value3");
        assertResolvePlaceholder(
                "param: value, param: $OTHER_PREFIX{param}",
                "param: $PREFIX{param}, param: $OTHER_PREFIX{param}",
                "PREFIX", "param", "value");
    }

    @Test
    void isNotBlank() {
        assertFalse(StringUtil.isNullOrEmptyAfterTrim("string"));
        // null unicode character
        assertFalse(StringUtil.isNullOrEmptyAfterTrim("\u0000"));
        // Non-breaking space
        assertFalse(StringUtil.isNullOrEmptyAfterTrim("\u00A0"));

        assertTrue(StringUtil.isNullOrEmptyAfterTrim("  "));
        assertTrue(StringUtil.isNullOrEmptyAfterTrim(""));
        // Em quad
        assertTrue(StringUtil.isNullOrEmptyAfterTrim("\u2001"));
        // Tab
        assertTrue(StringUtil.isNullOrEmptyAfterTrim("\t"));
        assertTrue(StringUtil.isNullOrEmptyAfterTrim("\n\r  "));
        assertTrue(StringUtil.isNullOrEmptyAfterTrim(null));
    }

    @Test
    void isAllFilledTest() {
        assertTrue(isAllNullOrEmptyAfterTrim("test-string-1", "test-string-2"));
        assertFalse(isAllNullOrEmptyAfterTrim("test-string-1", ""));
        assertFalse(isAllNullOrEmptyAfterTrim("", "", null));
    }

    @Test
    void isAnyFilledTest() {
        assertTrue(isAnyNullOrEmptyAfterTrim("test-string-1", "test-string-2"));
        assertTrue(isAnyNullOrEmptyAfterTrim("test-string-1", ""));
        assertFalse(isAnyNullOrEmptyAfterTrim("", "", null));
    }

    @Test
    void when_removingCharactersFromString_then_properValue() {
        assertEquals("", StringUtil.removeCharacter("-------", '-'));
        assertEquals("-------", StringUtil.removeCharacter("-------", '0'));
        assertEquals("-------", StringUtil.removeCharacter("-0-0-0-0-0-0-", '0'));
        assertEquals("-------", StringUtil.removeCharacter("-00000-0-0000-0000-0-0-", '0'));
    }

    @Test
    void when_removingNotExistingCharactersFromString_then_sameInstanceIsReturned() {
        assertSame("-------", StringUtil.removeCharacter("-------", '0'));
    }

    private void assertResolvePlaceholder(String expected,
                                          String pattern,
                                          String placeholderNamespace,
                                          Object... params) {
        HashMap<String, Object> paramMap = new HashMap<>();
        for (int i = 0; i < params.length; i += 2) {
            paramMap.put(params[i].toString(), params[i + 1]);
        }
        assertEquals(expected, StringUtil.resolvePlaceholders(pattern, placeholderNamespace, paramMap));
    }

    private String[] arr(String... strings) {
        return strings;
    }

    @Test
    void testStrip() {
        // Test with null input
        assertNull(StringUtil.strip(null));

        // Test with an empty string
        assertEquals("", StringUtil.strip(""));

        // Test with spaces
        assertEquals("Hello, World!", StringUtil.strip("  Hello, World!  "));

        // Test with Unicode spaces
        // Unicode character for em quad (\u2001) used for demonstration
        assertEquals("Hello, World!", StringUtil.strip("\u2001Hello, World!\u2001"));

        // Test with tabs and newlines
        assertEquals("Hello, World!", StringUtil.strip("\t\nHello, World!\t\n"));

        // Test with only leading spaces
        assertEquals("Hello, World!", StringUtil.strip("   Hello, World!"));

        // Test with only trailing spaces
        assertEquals("Hello, World!", StringUtil.strip("Hello, World!  "));

        // Test with a mix of spaces and Unicode characters
        assertEquals("Hello, World!", StringUtil.strip(" \tHello, World!\u2001  "));

        // Test with a string containing only spaces and Unicode characters
        assertEquals("", StringUtil.strip("  \t\u2001  "));
    }
}
