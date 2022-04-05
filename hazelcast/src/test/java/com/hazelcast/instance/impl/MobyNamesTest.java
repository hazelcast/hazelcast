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

package com.hazelcast.instance.impl;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MobyNamesTest extends HazelcastTestSupport {

    @Test
    public void getRandomNameNotEmpty() {
        String randomName = MobyNames.getRandomName(0);
        assertFalse(isNullOrEmptyAfterTrim(randomName));
    }

    @Test
    public void allValuesReturnedFair() {
        int totalCombinations = 98 * 240; // MobyNames.LEFT.length * MobyNames.RIGHT.length
        Map<String, AtomicInteger> namesCounts = new HashMap<>();
        for (int i = 0; i < totalCombinations * 2; i++) {
            String randomName = MobyNames.getRandomName(i);
            namesCounts.computeIfAbsent(randomName, (key) -> new AtomicInteger(0)).incrementAndGet();
        }
        assertEquals(namesCounts.size(), totalCombinations);
        assertTrue(namesCounts.keySet().stream().noneMatch(StringUtil::isNullOrEmptyAfterTrim));
        for (Map.Entry<String, AtomicInteger> entry : namesCounts.entrySet()) {
            assertEquals(entry.getKey(), 2, entry.getValue().get());
        }
    }
}
