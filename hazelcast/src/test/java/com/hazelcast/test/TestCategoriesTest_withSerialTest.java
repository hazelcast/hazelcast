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

package com.hazelcast.test;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TestCategoriesTest_withSerialTest extends HazelcastTestSupport {

    @Test
    public void testGetTestCategories() {
        HashSet<Class<?>> testCategories = getTestCategories();
        assertEquals("Expected a single test category", 1, testCategories.size());
        assertTrue("Expected to find a QuickTest category", testCategories.contains(QuickTest.class));
    }

    @Test
    public void testAssertThatNotMultithreadedTest() {
        try {
            assertThatIsNotMultithreadedTest();
            fail("Expected an exception on this serial test");
        } catch (AssertionError e) {
            ignore(e);
        }
    }
}
