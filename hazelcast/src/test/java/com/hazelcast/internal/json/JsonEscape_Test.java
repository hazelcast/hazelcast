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

package com.hazelcast.internal.json;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonEscape_Test {

    private StringBuilder stringBuilder;

    @Before
    public void setUp() {
        stringBuilder = new StringBuilder();
    }

    @Test
    public void writeMemberName_escapesBackslashes() {
        JsonEscape.writeEscaped(stringBuilder, "foo\\bar");

        assertEquals("\"foo\\\\bar\"", stringBuilder.toString());
    }

    @Test
    public void escapesQuotes() {
        JsonEscape.writeEscaped(stringBuilder, "a\"b");

        assertEquals("\"a\\\"b\"", stringBuilder.toString());
    }

    @Test
    public void escapesEscapedQuotes() {
        JsonEscape.writeEscaped(stringBuilder, "foo\\\"bar");

        assertEquals("\"foo\\\\\\\"bar\"", stringBuilder.toString());
    }

    @Test
    public void escapesNewLine() {
        JsonEscape.writeEscaped(stringBuilder, "foo\nbar");

        assertEquals("\"foo\\nbar\"", stringBuilder.toString());
    }

    @Test
    public void escapesWindowsNewLine() {
        JsonEscape.writeEscaped(stringBuilder, "foo\r\nbar");

        assertEquals("\"foo\\r\\nbar\"", stringBuilder.toString());
    }

    @Test
    public void escapesTabs() {
        JsonEscape.writeEscaped(stringBuilder, "foo\tbar");

        assertEquals("\"foo\\tbar\"", stringBuilder.toString());
    }

    @Test
    public void escapesSpecialCharacters() {
        JsonEscape.writeEscaped(stringBuilder, "foo\u2028bar\u2029");

        assertEquals("\"foo\\u2028bar\\u2029\"", stringBuilder.toString());
    }

    @Test
    public void escapesZeroCharacter() {
        JsonEscape.writeEscaped(stringBuilder, string('f', 'o', 'o', (char) 0, 'b', 'a', 'r'));

        assertEquals("\"foo\\u0000bar\"", stringBuilder.toString());
    }

    @Test
    public void escapesEscapeCharacter() {
        JsonEscape.writeEscaped(stringBuilder, string('f', 'o', 'o', (char) 27, 'b', 'a', 'r'));

        assertEquals("\"foo\\u001bbar\"", stringBuilder.toString());
    }

    @Test
    public void escapesControlCharacters() {
        JsonEscape.writeEscaped(stringBuilder, string((char) 1, (char) 8, (char) 15, (char) 16, (char) 31));

        assertEquals("\"\\u0001\\u0008\\u000f\\u0010\\u001f\"", stringBuilder.toString());
    }

    @Test
    public void escapesFirstChar() {
        JsonEscape.writeEscaped(stringBuilder, string('\\', 'x'));

        assertEquals("\"\\\\x\"", stringBuilder.toString());
    }

    @Test
    public void escapesLastChar() {
        String x = string('x', '\\');
        System.out.println(x);
        JsonEscape.writeEscaped(stringBuilder, x);

        assertEquals("\"x\\\\\"", stringBuilder.toString());
    }

    @Test
    public void escapesEscapeChar() {
        JsonEscape.writeEscaped(stringBuilder, '\\');

        assertEquals("\"\\\\\"", stringBuilder.toString());
    }

    @Test
    public void escapesNewLineChar() {
        JsonEscape.writeEscaped(stringBuilder, '\n');

        assertEquals("\"\\n\"", stringBuilder.toString());
    }

    private static String string(char... chars) {
        return String.valueOf(chars);
    }

}
