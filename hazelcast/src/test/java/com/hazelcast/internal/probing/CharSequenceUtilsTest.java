package com.hazelcast.internal.probing;

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
