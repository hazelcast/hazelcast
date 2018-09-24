/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CharSequenceUtilsTest {

    @Test
    public void appendEscaped() {
        assertEscapes("", "");
        assertEscapes("aaa", "aaa");
        assertEscapes("=", "\\=");
        assertEscapes(",", "\\,");
        assertEscapes("\\", "\\\\");
        assertEscapes("a=b", "a\\=b");
        assertEscapes("=b", "\\=b");
        assertEscapes("a=", "a\\=");
        assertEscapes("foo bar", "foo\\ bar");
    }

    private static void assertEscapes(String unescaped, String escaped) {
        StringBuilder buf = new StringBuilder();
        CharSequenceUtils.appendEscaped(buf, unescaped);
        assertEquals(escaped, buf.toString());
    }

    @Test
    public void appendUnescaped() {
        assertUnescapes("", "");
        assertUnescapes("aaa", "aaa");
        assertUnescapes("a\na", "a\na");
        assertUnescapes("a\\\na", "a\na");
        assertUnescapes("\\\na", "\na");
        assertUnescapes("a\\\n", "a\n");
        assertUnescapes("\\\n", "\n");
    }

    private static void assertUnescapes(String escaped, String unescaped) {
        StringBuilder buf = new StringBuilder();
        CharSequenceUtils.appendUnescaped(buf, escaped);
        assertEquals(unescaped, buf.toString());
    }

    @Test
    public void startsWith() {
        assertStartsWith("a", "");
        assertStartsWith("a", "a");
        assertStartsWith("ab", "a");
        assertStartsWith("ab", "ab");
        assertStartsWith("abc", "a");
        assertStartsWith("abc", "ab");
        assertStartsWith("abc", "abc");
        assertNotStartsWith("a", "b");
        assertNotStartsWith("ab", "ac");
        assertNotStartsWith("abc", "ac");
    }

    private static void assertStartsWith(String str, String prefix) {
        assertTrue(CharSequenceUtils.startsWith(prefix, str));
    }

    private static void assertNotStartsWith(String str, String prefix) {
        assertFalse(CharSequenceUtils.startsWith(prefix, str));
    }

    @Test
    public void parseLong() {
        assertLong(0, "");
        assertLong(1, "1");
        assertLong(1, "+1");
        assertLong(-1, "-1");
        assertLong(42, "42");
        assertLong(Long.MAX_VALUE, Long.toString(Long.MAX_VALUE));
        assertLong(Long.MIN_VALUE, Long.toString(Long.MIN_VALUE));
    }

    @Test(expected = NumberFormatException.class)
    public void parseLong_IllegalInput() {
        CharSequenceUtils.parseLong("a");
    }

    private static void assertLong(long expected, String actual) {
        assertEquals(expected, CharSequenceUtils.parseLong(actual));
    }
}
