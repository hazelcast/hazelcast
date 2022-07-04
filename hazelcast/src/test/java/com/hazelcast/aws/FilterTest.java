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

package com.hazelcast.aws;

import org.junit.Test;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class FilterTest {

    @Test
    public void add() {
        // given
        Filter filter = new Filter();

        // when
        filter.add("key", "value");
        filter.add("second-key", "second-value");
        Map<String, String> result = filter.getFilterAttributes();

        // then
        assertEquals(4, result.size());
        assertEquals("key", result.get("Filter.1.Name"));
        assertEquals("value", result.get("Filter.1.Value.1"));
        assertEquals("second-key", result.get("Filter.2.Name"));
        assertEquals("second-value", result.get("Filter.2.Value.1"));
    }

    @Test
    public void addMulti() {
        // given
        Filter filter = new Filter();

        // when
        filter.addMulti("key", asList("value", "second-value"));

        // then
        Map<String, String> result = filter.getFilterAttributes();

        // then
        assertEquals(3, result.size());
        assertEquals("key", result.get("Filter.1.Name"));
        assertEquals("value", result.get("Filter.1.Value.1"));
        assertEquals("second-value", result.get("Filter.1.Value.2"));
    }

}
