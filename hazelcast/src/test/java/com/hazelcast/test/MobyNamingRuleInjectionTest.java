/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.impl.MobyNames.MOBY_NAMING_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MobyNamingRuleInjectionTest {

    @Test
    public void testMobyNamingRuleAppliedByRunner() {
        System.clearProperty(MOBY_NAMING_PREFIX);

        Result result = JUnitCore.runClasses(SampleTest.class);

        assertTrue(result.wasSuccessful());

        // ensures cleanup happened after inner execution
        assertNull(System.getProperty(MOBY_NAMING_PREFIX));
    }

    @RunWith(HazelcastParallelClassRunner.class)
    public static class SampleTest {

        @Test
        public void test() {
            String value = System.getProperty(MOBY_NAMING_PREFIX);

            // Rule was applied by runner BEFORE test execution
            assertEquals(
                    SampleTest.class.getSimpleName(),
                    value
            );
        }
    }
}
