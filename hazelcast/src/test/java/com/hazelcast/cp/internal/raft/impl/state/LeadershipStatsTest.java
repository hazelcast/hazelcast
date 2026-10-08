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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.internal.util.Clock;
import org.junit.Test;

import static java.util.Arrays.stream;
import static org.junit.Assert.assertEquals;

public class LeadershipStatsTest {

    @Test
    public void basicTest() {
        long now = Clock.currentTimeMillis();
        long afterFiveSeconds = now + 5000;
        RaftState.LeadershipStats stats = new RaftState.LeadershipStats();
        stats.onLeaderElected(now);
        stats.onFollower(afterFiveSeconds);

        assertEquals(5000, stats.getAvgTimeAsLeaderMs());
        assertEquals(1, stats.getElectedLeaderCount());
    }

    @Test
    public void testAverages() {
        long[] electionDurations = {5000, 10000, 15000};
        long startTime = Clock.currentTimeMillis();

        RaftState.LeadershipStats stats = new RaftState.LeadershipStats();

        long electionTime = startTime;
        for (long duration : electionDurations) {
            stats.onLeaderElected(electionTime);
            stats.onFollower(electionTime + duration);
            electionTime += 100000; // Gap before the next election
        }

        long expectedAvg = stream(electionDurations).sum() / electionDurations.length;

        assertEquals(expectedAvg, stats.getAvgTimeAsLeaderMs());
        assertEquals(electionDurations.length, stats.getElectedLeaderCount());
    }
}
