/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsCollectionContextTest {

    @Test
    public void testMetricsCollectionContext() {
        MetricsCollectionContext context = new MetricsCollectionContext();
        context.collect(() -> "1", "hazelcast");
        context.collect(() -> "2", "phonehome");
        Map<String, String> map = context.getParameters();
        assertEquals("1=hazelcast&2=phonehome", context.getQueryString());
        assertEquals(Map.of("1", "hazelcast", "2", "phonehome"), map);
    }

    @Test
    public void testEmptyParameter() {
        MetricsCollectionContext context = new MetricsCollectionContext();
        Map<String, String> map = context.getParameters();
        assertEquals(Collections.emptyMap(), map);
        assertEquals("", context.getQueryString());
    }

    @Test
    public void checkDuplicateKey() {
        MetricsCollectionContext context = new MetricsCollectionContext();
        context.collect(() -> "1", "hazelcast");

        assertThatThrownBy(() -> context.collect(() -> "1", "phonehome"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Parameter 1 is already added");
    }
}
