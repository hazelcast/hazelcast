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
package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.FixedCapacityIntArrayList;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FixedCapacityIntArrayListTest {
    private static final int TEST_ARRAY_SIZE = 20;

    @Test
    public void when_elementsAdded_then_properSizeArray() {
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            FixedCapacityIntArrayList fixedCapacityIntArrayList = new FixedCapacityIntArrayList(TEST_ARRAY_SIZE);
            for (int j = 0; j < i; j++) {
                fixedCapacityIntArrayList.add(1);
            }
            assertEquals(i, fixedCapacityIntArrayList.asArray().length);
        }
    }

    @Test
    public void when_elementsAdded_then_properElementsInArray() {
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            FixedCapacityIntArrayList fixedCapacityIntArrayList = new FixedCapacityIntArrayList(TEST_ARRAY_SIZE);
            for (int j = 0; j < i; j++) {
                fixedCapacityIntArrayList.add(j);
            }
            int[] filledElements = fixedCapacityIntArrayList.asArray();
            for (int j = 0; j < i; j++) {
                assertEquals(j, filledElements[j]);
            }
        }
    }
}
