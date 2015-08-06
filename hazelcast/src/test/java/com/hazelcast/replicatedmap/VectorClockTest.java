/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap;

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.impl.record.VectorClockTimestamp;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VectorClockTest extends HazelcastTestSupport {

    @Test
    public void testEmptyClockShouldNotHappensBeforeItself() throws Exception {
        VectorClockTimestamp clock = new VectorClockTimestamp();
        assertNotHappensBefore(clock, clock);
    }

    @Test
    public void testIdenticalClocksShouldNotHaveHappensBeforeRelation() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl localMember = new MemberImpl(new Address("127.0.0.1", 0));
        clock1 = clock1.incrementClock(localMember);
        clock2 = clock2.incrementClock(localMember);
        assertNotHappensBefore(clock1, clock2);
        assertNotHappensBefore(clock2, clock1);
    }

    @Test
    public void testClockShouldHappensBeforeWithIdenticalClockWithOneMoreUpdate() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl localMember = new MemberImpl(new Address("127.0.0.1", 0));
        clock1 = clock1.incrementClock(localMember);
        clock2 = clock2.incrementClock(localMember);
        clock2 = clock2.incrementClock(localMember);
        assertHappensBefore(clock1, clock2);
    }

    @Test
    public void testHappensBefore() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        MemberImpl member3 = new MemberImpl(new Address("127.0.0.3", 0));
        clock1 = clock1.incrementClock(member2);
        clock1 = clock1.incrementClock(member2);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member3);

        // we need to represent happens before with an enum not with boolean
        assertHappensBefore(clock1, clock2);
        assertNotHappensBefore(clock2, clock1);
    }

    @Test
    public void testConcurrentEventsShouldNotHaveHappensBeforeRelation() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        clock1 = clock1.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);
        // we need to represent happens before with an enum not with boolean
        assertNotHappensBefore(clock1, clock2);
        assertNotHappensBefore(clock2, clock1);
    }

    @Test
    public void testConcurrentEventsShouldNotHaveHappensBeforeRelation_1() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        MemberImpl member3 = new MemberImpl(new Address("127.0.0.3", 0));

        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member2);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member3);
        // we need to represent happens before with an enum not with boolean
        assertNotHappensBefore(clock1, clock2);
        assertNotHappensBefore(clock2, clock1);
    }

    @Test
    public void testConcurrentEventsShouldNotHaveHappensBeforeRelation_2() throws Exception {
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        MemberImpl member3 = new MemberImpl(new Address("127.0.0.3", 0));

        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member2);
        clock1 = clock1.incrementClock(member3);
        clock1 = clock1.incrementClock(member3);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member3);
        // we need to represent happens before with an enum not with boolean
        assertNotHappensBefore(clock1, clock2);
        assertNotHappensBefore(clock2, clock1);
    }

    @Test
    public void testMergeEmptyClockWithEmptyClock() throws Exception {
        VectorClockTimestamp clock = new VectorClockTimestamp();
        VectorClockTimestamp merged = clock.applyVector(clock);
        assertEquals(clock, merged);
    }

    @Test
    public void testMergeClockWithItself() throws Exception {
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        VectorClockTimestamp clock = new VectorClockTimestamp();
        clock = clock.incrementClock(member1);
        VectorClockTimestamp merged = clock.applyVector(clock);
        assertEquals(clock, merged);
    }

    @Test
    public void testMergeConcurrentEvents() throws Exception {
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        VectorClockTimestamp manuallyMerged = new VectorClockTimestamp();

        clock1 = clock1.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);

        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member2);

        assertEquals(manuallyMerged, clock1.applyVector(clock2));
        assertEquals(manuallyMerged, clock2.applyVector(clock1));
    }

    @Test
    public void testMergeSubset() throws Exception {
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        VectorClockTimestamp manuallyMerged = new VectorClockTimestamp();

        clock1 = clock1.incrementClock(member1);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);

        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member2);

        assertEquals(manuallyMerged, clock1.applyVector(clock2));
        assertEquals(manuallyMerged, clock2.applyVector(clock1));
    }

    @Test
    public void testTwoWayMerge() throws Exception {
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        MemberImpl member3 = new MemberImpl(new Address("127.0.0.3", 0));
        MemberImpl member4 = new MemberImpl(new Address("127.0.0.4", 0));
        MemberImpl member5 = new MemberImpl(new Address("127.0.0.5", 0));
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        VectorClockTimestamp manuallyMerged = new VectorClockTimestamp();

        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member1);
        clock1 = clock1.incrementClock(member2);
        clock1 = clock1.incrementClock(member3);
        clock1 = clock1.incrementClock(member5);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member4);

        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member2);
        manuallyMerged = manuallyMerged.incrementClock(member2);
        manuallyMerged = manuallyMerged.incrementClock(member3);
        manuallyMerged = manuallyMerged.incrementClock(member4);
        manuallyMerged = manuallyMerged.incrementClock(member5);

        assertEquals(manuallyMerged, clock1.applyVector(clock2));
        assertEquals(manuallyMerged, clock2.applyVector(clock1));
    }

    @Test
    public void testTwoWayMerge_2() throws Exception {
        MemberImpl member1 = new MemberImpl(new Address("127.0.0.1", 0));
        MemberImpl member2 = new MemberImpl(new Address("127.0.0.2", 0));
        MemberImpl member3 = new MemberImpl(new Address("127.0.0.3", 0));
        MemberImpl member4 = new MemberImpl(new Address("127.0.0.4", 0));
        MemberImpl member5 = new MemberImpl(new Address("127.0.0.5", 0));
        MemberImpl member6 = new MemberImpl(new Address("127.0.0.6", 0));
        VectorClockTimestamp clock1 = new VectorClockTimestamp();
        VectorClockTimestamp clock2 = new VectorClockTimestamp();
        VectorClockTimestamp manuallyMerged = new VectorClockTimestamp();

        clock1 = clock1.incrementClock(member2);
        clock1 = clock1.incrementClock(member3);
        clock1 = clock1.incrementClock(member5);

        clock2 = clock2.incrementClock(member1);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member2);
        clock2 = clock2.incrementClock(member4);
        clock2 = clock2.incrementClock(member6);

        manuallyMerged = manuallyMerged.incrementClock(member1);
        manuallyMerged = manuallyMerged.incrementClock(member2);
        manuallyMerged = manuallyMerged.incrementClock(member2);
        manuallyMerged = manuallyMerged.incrementClock(member3);
        manuallyMerged = manuallyMerged.incrementClock(member4);
        manuallyMerged = manuallyMerged.incrementClock(member5);
        manuallyMerged = manuallyMerged.incrementClock(member6);

        assertEquals(manuallyMerged, clock1.applyVector(clock2));
        assertEquals(manuallyMerged, clock2.applyVector(clock1));
    }
}
