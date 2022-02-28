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

package com.hazelcast.jet.impl.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class EnumerationUtilTest {

    @Test
    public void test_stream_from_enumeration() {
        Enumeration<Integer> someEnumeration = Collections.enumeration(Arrays.asList(1, 2, 3));
        assertThat(EnumerationUtil.stream(someEnumeration).collect(Collectors.toList())).containsOnlyOnce(1, 2, 3);
    }

    @Test
    public void test_stream_from_empty_enumeration() {
        Enumeration<Integer> someEnumeration = Collections.enumeration(Collections.emptyList());
        assertThat(EnumerationUtil.stream(someEnumeration).collect(Collectors.toList())).isEmpty();
    }

    @Test
    public void test_count_stream_from_enumeration() {
        Enumeration<Integer> someEnumeration = Collections.enumeration(Arrays.asList(1, 2, 3));
        assertThat(EnumerationUtil.stream(someEnumeration).count()).isEqualTo(3);
    }
}
