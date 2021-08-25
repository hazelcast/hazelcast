/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JsonEscape_Test {

    @Test
    public void escapeBackSlashes() throws IOException {
        assertEquals("\"foo\\\\bar\"", JsonEscape.escape(("foo\\bar")));
    }

    @Test
    public void escapesQuotes() throws IOException {
        assertEquals("\"a\\\"b\"", JsonEscape.escape("a\"b"));
    }

    @Test
    public void escapesEscapedQuotes() throws IOException {
        assertEquals("\"foo\\\\\\\"bar\"", JsonEscape.escape("foo\\\"bar"));
    }

    @Test
    public void escapesNewLine() throws IOException {
        assertEquals("\"foo\\nbar\"", JsonEscape.escape("foo\nbar"));
    }

    @Test
    public void escapesWindowsNewLine() throws IOException {
        assertEquals("\"foo\\r\\nbar\"", JsonEscape.escape("foo\r\nbar"));
    }

    @Test
    public void escapesTabs() throws IOException {
        assertEquals("\"foo\\tbar\"", JsonEscape.escape("foo\tbar"));
    }

    @Test
    public void escapesSpecialCharacters() throws IOException {
        assertEquals("\"foo\\u2028bar\\u2029\"", JsonEscape.escape("foo\u2028bar\u2029"));
    }

    @Test
    public void escapesZeroCharacter() throws IOException {
        assertEquals("\"foo\\u0000bar\"", JsonEscape.escape(string('f', 'o', 'o', (char) 0, 'b', 'a', 'r')));
    }

    @Test
    public void escapesEscapeCharacter() throws IOException {
        assertEquals("\"foo\\u001bbar\"", JsonEscape.escape(string('f', 'o', 'o', (char) 27, 'b', 'a', 'r')));
    }

    @Test
    public void escapesControlCharacters() throws IOException {
        assertEquals("\"\\u0001\\u0008\\u000f\\u0010\\u001f\"", JsonEscape.escape(string((char) 1, (char) 8, (char) 15, (char) 16, (char) 31)));
    }

    @Test
    public void escapesFirstChar() throws IOException {
        assertEquals("\"\\\\x\"", JsonEscape.escape(string('\\', 'x')));
    }

    @Test
    public void escapesLastChar() throws IOException {
        assertEquals("\"x\\\\\"", JsonEscape.escape(string('x', '\\')));
    }

    private static String string(char... chars) {
        return String.valueOf(chars);
    }

}
