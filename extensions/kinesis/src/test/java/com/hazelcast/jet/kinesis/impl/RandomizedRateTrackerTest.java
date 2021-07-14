/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RandomizedRateTrackerTest {

    @Test
    public void check() {
        RandomizedRateTracker tracker = new RandomizedRateTracker(1000, 5);
        long p1 = tracker.next();
        long p2 = tracker.next();
        long p3 = tracker.next();
        long p4 = tracker.next();
        long p5 = tracker.next();

        //total is divided up properly
        assertEquals(1000, p1 + p2 + p3 + p4 + p5);

        //returns same values in a round-robin fashion
        for (int i = 0; i < 5; i++) {
            assertEquals(p1, tracker.next());
            assertEquals(p2, tracker.next());
            assertEquals(p3, tracker.next());
            assertEquals(p4, tracker.next());
            assertEquals(p5, tracker.next());
        }
    }

}
