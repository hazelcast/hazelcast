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

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricKeyPart;
import static com.hazelcast.internal.metrics.MetricsUtil.parseMetricKey;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
public class MetricsUtilTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_escapeMetricKeyPart() {
        assertSame("", escapeMetricKeyPart(""));
        assertSame("aaa", escapeMetricKeyPart("aaa"));
        assertEquals("\\=", escapeMetricKeyPart("="));
        assertEquals("\\,", escapeMetricKeyPart(","));
        assertEquals("\\\\", escapeMetricKeyPart("\\"));
        assertEquals("a\\=b", escapeMetricKeyPart("a=b"));
        assertEquals("\\=b", escapeMetricKeyPart("=b"));
        assertEquals("a\\=", escapeMetricKeyPart("a="));
    }

    @Test
    public void test_parseMetricKey() {
        // empty list
        assertEquals(emptyList(), parseMetricKey("[]"));
        // normal single tag
        assertEquals(singletonList(entry("tag", "value")), parseMetricKey("[tag=value]"));
        // normal multiple tags
        assertEquals(asList(entry("tag1", "value1"), entry("tag2", "value2")),
                parseMetricKey("[tag1=value1,tag2=value2]"));
        // empty value
        assertEquals(singletonList(entry("tag=", "value,")), parseMetricKey("[tag\\==value\\,]"));
    }

    @Test
    public void test_parseMetricKey_fail_keyNotEnclosed() {
        exception.expectMessage("key not enclosed in []");
        parseMetricKey("tag=value");
    }

    @Test
    public void test_parseMetricKey_fail_emptyTagName() {
        exception.expectMessage("empty tag name");
        parseMetricKey("[=value]");
    }

    @Test
    public void test_parseMetricKey_fail_equalsSignAfterValue() {
        exception.expectMessage("equals sign not after tag");
        parseMetricKey("[tag=value=]");
    }

    @Test
    public void test_parseMetricKey_fail_commaInTag1() {
        exception.expectMessage("comma in tag");
        parseMetricKey("[,]");
    }

    @Test
    public void test_parseMetricKey_fail_backslashAtTheEnd1() {
        exception.expectMessage("backslash at the end");
        parseMetricKey("[\\]");
    }

    @Test
    public void test_parseMetricKey_fail_backslashAtTheEnd2() {
        exception.expectMessage("backslash at the end");
        parseMetricKey("[tag=value\\]");
    }

    @Test
    public void test_parseMetricKey_fail_unfinishedTagAtTheEnd() {
        exception.expectMessage("unfinished tag at the end");
        parseMetricKey("[tag=value,tag2]");
    }

    private Entry<String, String> entry(String key, String value) {
        return new SimpleImmutableEntry<String, String>(key, value);
    }
}
