/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricNamePart;
import static com.hazelcast.internal.metrics.MetricsUtil.parseMetricName;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetricsUtilTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void test_escapeMetricKeyPart() {
        assertSame("", escapeMetricNamePart(""));
        assertSame("aaa", escapeMetricNamePart("aaa"));
        assertEquals("\\=", escapeMetricNamePart("="));
        assertEquals("\\,", escapeMetricNamePart(","));
        assertEquals("\\\\", escapeMetricNamePart("\\"));
        assertEquals("a\\=b", escapeMetricNamePart("a=b"));
        assertEquals("\\=b", escapeMetricNamePart("=b"));
        assertEquals("a\\=", escapeMetricNamePart("a="));
    }

    @Test
    public void test_parseMetricKey() {
        // empty list
        assertEquals(emptyList(), parseMetricName("[]"));
        // normal single tag
        assertEquals(singletonList(entry("tag", "value")), parseMetricName("[tag=value]"));
        // normal multiple tags
        assertEquals(asList(entry("tag1", "value1"), entry("tag2", "value2")),
                parseMetricName("[tag1=value1,tag2=value2]"));
        // tag with escaped characters
        assertEquals(singletonList(entry("tag=", "value,")), parseMetricName("[tag\\==value\\,]"));
    }

    @Test
    public void test_parseMetricKey_fail_keyNotEnclosed() {
        exception.expectMessage("key not enclosed in []");
        parseMetricName("tag=value");
    }

    @Test
    public void test_parseMetricKey_fail_emptyTagName() {
        exception.expectMessage("empty tag name");
        parseMetricName("[=value]");
    }

    @Test
    public void test_parseMetricKey_fail_equalsSignAfterValue() {
        exception.expectMessage("equals sign not after tag");
        parseMetricName("[tag=value=]");
    }

    @Test
    public void test_parseMetricKey_fail_commaInTag1() {
        exception.expectMessage("comma in tag");
        parseMetricName("[,]");
    }

    @Test
    public void test_parseMetricKey_fail_backslashAtTheEnd1() {
        exception.expectMessage("backslash at the end");
        parseMetricName("[\\]");
    }

    @Test
    public void test_parseMetricKey_fail_backslashAtTheEnd2() {
        exception.expectMessage("backslash at the end");
        parseMetricName("[tag=value\\]");
    }

    @Test
    public void test_parseMetricKey_fail_unfinishedTagAtTheEnd() {
        exception.expectMessage("unfinished tag at the end");
        parseMetricName("[tag=value,tag2]");
    }

    private static Entry<String, String> entry(String key, String value) {
        return new SimpleImmutableEntry<String, String>(key, value);
    }
}
