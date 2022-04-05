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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SplitBrainTestSupportTest extends SplitBrainTestSupport {

    private static final int FIRST_HALF = 1;
    private static final int SECOND_HALF = 2;

    @Override
    protected int[] brains() {
        return new int[]{FIRST_HALF, SECOND_HALF};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        int expectedClusterSize = FIRST_HALF + SECOND_HALF;
        for (HazelcastInstance hz : instances) {
            assertClusterSize(expectedClusterSize, hz);
        }
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstHalf, HazelcastInstance[] secondHalf) {
        for (HazelcastInstance hz : firstHalf) {
            assertClusterSize(FIRST_HALF, hz);
        }
        for (HazelcastInstance hz : secondHalf) {
            assertClusterSize(SECOND_HALF, hz);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] allInstances) {
        for (HazelcastInstance hz : allInstances) {
            assertClusterSize(FIRST_HALF + SECOND_HALF, hz);
        }
    }
}
