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

package com.hazelcast.instance.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MobyNamesTest extends HazelcastTestSupport {

    @Test
    public void getRandomNameNotEmpty() {
        String randomName = MobyNames.getRandomName();
        assertFalse(isNullOrEmptyAfterTrim(randomName));
    }

    @Test
    public void getNextRandomNameShouldBeDifferentWithHighProbability() {
        String randomName = MobyNames.getRandomName();
        int attempts = 100;
        for (int i = 0; i < attempts; i++) {
            if (!randomName.equals(MobyNames.getRandomName())) {
                return;
            }
        }
        fail("Could not generate the name that is not equal to " + randomName + " in " + attempts + " attempts");
    }
}
