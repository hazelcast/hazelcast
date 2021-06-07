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

package com.hazelcast.internal.ascii;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HttpContentTypeParserTest extends HazelcastTestSupport {
    //reference: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.1.1
    @Test
    public void parseCharset_NullContentType() {
        assertNull(HttpContentTypeParser.parseCharset(null));
    }

    @Test
    public void parseCharset_MissingCharsetField() {
        assertNull(HttpContentTypeParser.parseCharset("application/json"));
    }

    @Test
    public void parseCharset_MissingCharsetValue() {
        assertNull(HttpContentTypeParser.parseCharset("application/json charset="));
    }

    @Test
    public void parseCharset_InvalidCharsetField() {
        assertNull(HttpContentTypeParser.parseCharset("foocharset=UTF"));
    }

    @Test
    public void parseCharset_HappyCharset() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html; charset=UTF-8");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_CharsetFieldAfterSemicolon() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html;charset=utf-8");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_SpaceAfterEncoding() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html;charset=utf-8 ");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_SemicolonAfterEncoding() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html;charset=utf-8;");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_UnknownEncoding() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=8-ftu;"));
    }

    @Test
    public void parseCharset_CharsetFieldInUpperCare() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html;CHARSET=utf-8;");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_CharsetNameInDoubleQuotes() {
        Charset charset = HttpContentTypeParser.parseCharset("text/html;charset=\"utf-8\"");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    public void parseCharset_FirstDoubleQuoteInsideCharset() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=u\"tf-8\";"));
    }

    @Test
    public void parseCharset_SecondDoubleQuoteInsideCharset() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=\"utf-\"8;"));
    }

    @Test
    public void parseCharset_NoMatchingDoubleQuote() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=\"utf-8;"));
    }

    @Test
    public void parseCharset_MissingCharsetValueInDoubleQuotes() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=\"\""));
    }

    @Test
    public void parseCharset_MissingCharsetValueInDoubleQuotesAndNoMatching() {
        assertNull(HttpContentTypeParser.parseCharset("text/html;charset=\""));
    }

    @Test
    public void parseCharset_MissingMediaType() {
        assertNull(HttpContentTypeParser.parseCharset("charset=utf-8"));
    }

    @Test
    public void parseMediaType_NullContentType() {
        assertNull(HttpContentTypeParser.parseMediaType(null));
    }

    @Test
    public void parseMediaType_HappyCase() {
        String s = HttpContentTypeParser.parseMediaType("text/html; charset=UTF-8");
        assertEquals("text/html", s);
    }

    @Test
    public void parseMediaType_PlainMediaType() {
        String s = HttpContentTypeParser.parseMediaType("text/html");
        assertEquals("text/html", s);
    }

    @Test
    public void parseMediaType_SpaceActAsASeparator() {
        String s = HttpContentTypeParser.parseMediaType("text/html bar");
        assertEquals("text/html", s);
    }

    @Test
    public void parseMediaType_MediaTypeInUpperCase() {
        String s = HttpContentTypeParser.parseMediaType("TEXT/HTML");
        assertEquals("text/html", s);
    }

    @Test
    public void parseMediaType_NoSubtype() {
        assertNull(HttpContentTypeParser.parseMediaType("TEXT"));
    }

    @Test
    public void parseMediaType_NotStartingWithMediaType() {
        assertNull(HttpContentTypeParser.parseMediaType("texthtml fds/bar"));
    }

    @Test
    public void parseMediaType_MissingType() {
        assertNull(HttpContentTypeParser.parseMediaType("/html"));
    }

    @Test
    public void parseMediaType_MissingSubType() {
        assertNull(HttpContentTypeParser.parseMediaType("text/"));
    }

    @Test
    public void parseMediaType_MissingSubTypeAndType() {
        assertNull(HttpContentTypeParser.parseMediaType("/"));
    }
}
