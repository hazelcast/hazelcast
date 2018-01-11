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

package com.hazelcast.test;

import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TestCategoriesTest_withParallelTest extends HazelcastTestSupport {

    @Test
    public void testGetTestCategories() {
        HashSet<Class<?>> testCategories = getTestCategories();
        assertEquals("Expected a two test categories", 2, testCategories.size());
        assertTrue("Expected to find a QuickTest category", testCategories.contains(QuickTest.class));
        assertTrue("Expected to find a ParallelTest category", testCategories.contains(ParallelTest.class));
    }

    @Test(expected = AssertionError.class)
    public void testAssertThatIsNoParallelTest() {
        assertThatIsNoParallelTest();
        throw new RuntimeException("Expected an AssertionError on this ParallelTest");
    }
}
