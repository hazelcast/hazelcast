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

package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiResultTest extends HazelcastTestSupport {

    private MultiResult<Object> result = new MultiResult<Object>();

    @Test
    public void addNull() {
        result.add(null);

        assertEquals(1, result.getResults().size());
        assertContains(result.getResults(), null);
    }

    @Test
    public void addValue() {
        result.add("007");

        assertEquals(1, result.getResults().size());
        assertContains(result.getResults(), "007");
    }

    @Test
    public void empty() {
        assertTrue(result.isEmpty());
    }

    @Test
    public void nonEmpty() {
        result.add("007");

        assertFalse(result.isEmpty());
    }

    @Test
    public void noLitter() {
        List<String> strings = asList("James", "Bond", "007");

        MultiResult<String> result = new MultiResult<String>(strings);

        assertSame(strings, result.getResults());
    }
}
