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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Locale;

import static com.hazelcast.internal.util.StringUtil.VERSION_PATTERN;
import static com.hazelcast.internal.util.StringUtil.isAllNullOrEmptyAfterTrim;
import static com.hazelcast.internal.util.StringUtil.isAnyNullOrEmptyAfterTrim;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StringUtilTest extends HazelcastTestSupport {

    @Test
    public void testVersionPattern() {
        assertTrue(VERSION_PATTERN.matcher("3.1").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-RC").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-RC1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-RC").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-RC1-SNAPSHOT").matches());

        assertFalse(VERSION_PATTERN.matcher("${project.version}").matches());
        assertFalse(VERSION_PATTERN.matcher("project.version").matches());
        assertFalse(VERSION_PATTERN.matcher("3").matches());
        assertFalse(VERSION_PATTERN.matcher("3.RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3.SNAPSHOT").matches());
        assertFalse(VERSION_PATTERN.matcher("3-RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3-SNAPSHOT").matches());
        assertFalse(VERSION_PATTERN.matcher("3.").matches());
        assertFalse(VERSION_PATTERN.matcher("3.1.RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3.1.SNAPSHOT").matches());
    }

    @Test
    public void getterIntoProperty_whenNull_returnNull() throws Exception {
        assertEquals("", StringUtil.getterIntoProperty(""));
    }

    @Test
    public void getterIntoProperty_whenEmpty_returnEmptyString() throws Exception {
        assertEquals("", StringUtil.getterIntoProperty(""));
    }

    @Test
    public void getterIntoProperty_whenGet_returnUnchanged() throws Exception {
        assertEquals("get", StringUtil.getterIntoProperty("get"));
    }

    @Test
    public void getterIntoProperty_whenGetFoo_returnFoo() throws Exception {
        assertEquals("foo", StringUtil.getterIntoProperty("getFoo"));
    }

    @Test
    public void getterIntoProperty_whenGetF_returnF() throws Exception {
        assertEquals("f", StringUtil.getterIntoProperty("getF"));
    }

    @Test
    public void getterIntoProperty_whenGetNumber_returnNumber() throws Exception {
        assertEquals("8", StringUtil.getterIntoProperty("get8"));
    }

    @Test
    public void getterIntoProperty_whenPropertyIsLowerCase_DoNotChange() throws Exception {
        assertEquals("getfoo", StringUtil.getterIntoProperty("getfoo"));
    }

    @Test
    public void test_lowerCaseFirstChar() {
        assertEquals("", StringUtil.lowerCaseFirstChar(""));
        assertEquals(".", StringUtil.lowerCaseFirstChar("."));
        assertEquals(" ", StringUtil.lowerCaseFirstChar(" "));
        assertEquals("a", StringUtil.lowerCaseFirstChar("a"));
        assertEquals("a", StringUtil.lowerCaseFirstChar("A"));
        assertEquals("aBC", StringUtil.lowerCaseFirstChar("ABC"));
        assertEquals("abc", StringUtil.lowerCaseFirstChar("Abc"));
    }

    @Test
    public void testSplitByComma() throws Exception {
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
    public void testArrayIntersection() throws Exception {
        assertArrayEquals(arr("test"), StringUtil.intersection(arr("x", "test", "y", "z"), arr("a", "b", "test")));
        assertArrayEquals(arr(""), StringUtil.intersection(arr("", "z"), arr("a", "")));
        assertArrayEquals(arr(), StringUtil.intersection(arr("", "z"), arr("a")));
    }

    @Test
    public void testArraySubraction() throws Exception {
        assertNull(StringUtil.subtraction(null, arr("a", "test", "b", "a")));
        assertArrayEquals(arr("a", "test", "b", "a"), StringUtil.subtraction(arr("a", "test", "b", "a"), null));
        assertArrayEquals(arr("test"), StringUtil.subtraction(arr("a", "test", "b", "a"), arr("a", "b")));
        assertArrayEquals(arr(), StringUtil.subtraction(arr(), arr("a", "b")));
        assertArrayEquals(arr("a", "b"), StringUtil.subtraction(arr("a", "b"), arr()));
        assertArrayEquals(arr(), StringUtil.subtraction(arr("a", "test", "b", "a"), arr("a", "b", "test")));
    }

    @Test
    public void testEqualsIgnoreCase() throws Exception {
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
    public void testStripTrailingSlash() throws Exception {
        assertEquals(null, StringUtil.stripTrailingSlash(null));
        assertEquals("", StringUtil.stripTrailingSlash(""));
        assertEquals("a", StringUtil.stripTrailingSlash("a"));
        assertEquals("a", StringUtil.stripTrailingSlash("a/"));
        assertEquals("a/a", StringUtil.stripTrailingSlash("a/a"));
        assertEquals("a/", StringUtil.stripTrailingSlash("a//"));
    }

    @Test
    public void testResolvePlaceholders() throws Exception {
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
    public void isNotBlank() {
        assertTrue(!StringUtil.isNullOrEmptyAfterTrim("string"));
        assertFalse(!StringUtil.isNullOrEmptyAfterTrim("  "));
        assertFalse(!StringUtil.isNullOrEmptyAfterTrim(""));
        assertFalse(!StringUtil.isNullOrEmptyAfterTrim(null));
    }

    @Test
    public void isAllFilledTest() {
        assertTrue(isAllNullOrEmptyAfterTrim("test-string-1", "test-string-2"));
        assertFalse(isAllNullOrEmptyAfterTrim("test-string-1", ""));
        assertFalse(isAllNullOrEmptyAfterTrim("", "", null));
    }

    @Test
    public void isAnyFilledTest() {
        assertTrue(isAnyNullOrEmptyAfterTrim("test-string-1", "test-string-2"));
        assertTrue(isAnyNullOrEmptyAfterTrim("test-string-1", ""));
        assertFalse(isAnyNullOrEmptyAfterTrim("", "", null));
    }

    @Test
    public void when_removingCharactersFromString_then_properValue() {
        assertEquals("", StringUtil.removeCharacter("-------", '-'));
        assertEquals("-------", StringUtil.removeCharacter("-------", '0'));
        assertEquals("-------", StringUtil.removeCharacter("-0-0-0-0-0-0-", '0'));
        assertEquals("-------", StringUtil.removeCharacter("-00000-0-0000-0000-0-0-", '0'));
    }

    @Test
    public void when_removingNotExistingCharactersFromString_then_sameInstanceIsReturned() {
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
}
