/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StringUtilTest extends HazelcastTestSupport {

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
        assertNull(StringUtil.subraction(null, arr("a", "test", "b", "a")));
        assertArrayEquals(arr("a", "test", "b", "a"), StringUtil.subraction(arr("a", "test", "b", "a"), null));
        assertArrayEquals(arr("test"), StringUtil.subraction(arr("a", "test", "b", "a"), arr("a", "b")));
        assertArrayEquals(arr(), StringUtil.subraction(arr(), arr("a", "b")));
        assertArrayEquals(arr("a", "b"), StringUtil.subraction(arr("a", "b"), arr()));
        assertArrayEquals(arr(), StringUtil.subraction(arr("a", "test", "b", "a"), arr("a", "b", "test")));
    }

    private String[] arr(String... strings) {
        return strings;
    }
}
