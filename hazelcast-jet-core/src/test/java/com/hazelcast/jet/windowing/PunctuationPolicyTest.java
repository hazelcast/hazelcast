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

package com.hazelcast.jet.windowing;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class PunctuationPolicyTest {

    private static final int MIN_STEP = 2;
    private long punc;
    private PunctuationPolicy p = new PunctuationPolicy() {

        @Override
        public long reportEvent(long timestamp) {
            return punc;
        }

        @Override
        public long getCurrentPunctuation() {
            return punc;
        }
    };

    @Test
    public void when_puncIncreasing_then_throttleByMinStep() {
        p = p.throttleByMinStep(MIN_STEP);
        assertPunc(2, 2);
        assertPunc(3, 2);
        assertPunc(4, 4);
        assertPunc(6, 6);
        assertPunc(9, 9);
        assertPunc(10, 9);
        assertPunc(11, 11);
    }

    @Test
    public void when_puncIncreasing_then_throttleByFrame() {
        WindowDefinition winDef = new WindowDefinition(3, 0, 1);
        p = p.throttleByFrame(winDef);
        assertPunc(Long.MIN_VALUE, Long.MIN_VALUE);
        assertPunc(2, 0);
        assertPunc(3, 3);
        assertPunc(4, 3);
        assertPunc(5, 3);
        assertPunc(6, 6);
        assertPunc(13, 12);
        assertPunc(14, 12);
        assertPunc(15, 15);
    }

    private void assertPunc(long actualPunc, long throttledPunc) {
        punc = actualPunc;
        assertEquals(throttledPunc, p.reportEvent(0));
        assertEquals(throttledPunc, p.getCurrentPunctuation());
    }
}
