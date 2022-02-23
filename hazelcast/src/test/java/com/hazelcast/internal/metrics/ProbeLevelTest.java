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

package com.hazelcast.internal.metrics;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ProbeLevelTest {

    @Test
    public void isEnabled() {
        assertTrue(MANDATORY.isEnabled(MANDATORY));
        assertTrue(MANDATORY.isEnabled(INFO));
        assertTrue(MANDATORY.isEnabled(DEBUG));

        assertFalse(INFO.isEnabled(MANDATORY));
        assertTrue(INFO.isEnabled(INFO));
        assertTrue(INFO.isEnabled(DEBUG));

        assertFalse(DEBUG.isEnabled(MANDATORY));
        assertFalse(DEBUG.isEnabled(INFO));
        assertTrue(DEBUG.isEnabled(DEBUG));
    }
}
