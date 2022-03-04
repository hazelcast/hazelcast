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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LeaderStateTest {

    private LeaderState state;
    private Set<RaftEndpoint> remoteEndpoints;
    private int lastLogIndex;

    @Before
    public void setUp() throws Exception {
        lastLogIndex = 123;
        remoteEndpoints = new HashSet<RaftEndpoint>(asList(
                newRaftMember(5001),
                newRaftMember(5002),
                newRaftMember(5003),
                newRaftMember(5004)));

        state = new LeaderState(remoteEndpoints, lastLogIndex);
    }

    @Test
    public void test_initialState() {
        for (RaftEndpoint endpoint : remoteEndpoints) {
            FollowerState followerState = state.getFollowerState(endpoint);
            assertEquals(0, followerState.matchIndex());
            assertEquals(lastLogIndex + 1, followerState.nextIndex());
        }

        long[] matchIndices = state.matchIndices();
        assertEquals(remoteEndpoints.size() + 1, matchIndices.length);
        for (long index : matchIndices) {
            assertEquals(0, index);
        }
    }

    @Test
    public void test_nextIndex() {
        Map<RaftEndpoint, Integer> indices = new HashMap<RaftEndpoint, Integer>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = 1 + RandomPicker.getInt(100);
            state.getFollowerState(endpoint).nextIndex(index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            int index = indices.get(endpoint);
            assertEquals(index, state.getFollowerState(endpoint).nextIndex());
        }
    }

    @Test
    public void test_matchIndex() {
        Map<RaftEndpoint, Long> indices = new HashMap<RaftEndpoint, Long>();
        for (RaftEndpoint endpoint : remoteEndpoints) {
            long index = 1 + RandomPicker.getInt(100);
            state.getFollowerState(endpoint).matchIndex(index);
            indices.put(endpoint, index);
        }

        for (RaftEndpoint endpoint : remoteEndpoints) {
            long index = indices.get(endpoint);
            assertEquals(index, state.getFollowerState(endpoint).matchIndex());
        }

        long[] matchIndices = state.matchIndices();
        assertEquals(indices.size() + 1, matchIndices.length);

        for (int i = 0; i < matchIndices.length - 1; i++) {
            long index = matchIndices[i];
            assertTrue(indices.containsValue(index));
        }
    }

}
