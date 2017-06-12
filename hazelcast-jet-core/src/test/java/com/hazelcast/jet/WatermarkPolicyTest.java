/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkPolicyTest {

    private static final int MIN_STEP = 2;
    private long wm;
    private WatermarkPolicy p = new WatermarkPolicy() {

        @Override
        public long reportEvent(long timestamp) {
            return wm;
        }

        @Override
        public long getCurrentWatermark() {
            return wm;
        }
    };

    @Test
    public void when_wmIncreasing_then_throttleByMinStep() {
        p = p.throttleByMinStep(MIN_STEP);
        assertWm(2, 2);
        assertWm(3, 2);
        assertWm(4, 4);
        assertWm(6, 6);
        assertWm(9, 9);
        assertWm(10, 9);
        assertWm(11, 11);
    }

    @Test
    public void when_wmIncreasing_then_throttleByFrame() {
        WindowDefinition winDef = new WindowDefinition(3, 0, 1);
        p = p.throttleByFrame(winDef);
        assertWm(Long.MIN_VALUE, Long.MIN_VALUE);
        assertWm(2, 0);
        assertWm(3, 3);
        assertWm(4, 3);
        assertWm(5, 3);
        assertWm(6, 6);
        assertWm(13, 12);
        assertWm(14, 12);
        assertWm(15, 15);
    }

    private void assertWm(long actualWm, long throttledWm) {
        wm = actualWm;
        assertEquals(throttledWm, p.reportEvent(0));
        assertEquals(throttledWm, p.getCurrentWatermark());
    }
}
