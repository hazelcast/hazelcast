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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CandidateStateTest {

    private CandidateState state;
    private int majority;

    @Before
    public void setUp() throws Exception {
        majority = 3;
        state = new CandidateState(majority);
    }

    @Test
    public void test_initialState() {
        assertEquals(majority, state.majority());
        assertEquals(0, state.voteCount());
        assertFalse(state.isMajorityGranted());
    }

    @Test
    public void test_grantVote_withoutMajority() {
        RaftEndpoint endpoint = newRaftMember(1000);

        assertTrue(state.grantVote(endpoint));
        assertFalse(state.grantVote(endpoint));

        assertEquals(1, state.voteCount());
        assertFalse(state.isMajorityGranted());
    }

    @Test
    public void test_grantVote_withMajority() {
        for (int i = 0; i < majority; i++) {
            RaftEndpoint endpoint = newRaftMember(1000 + i);
            assertTrue(state.grantVote(endpoint));

        }
        assertEquals(majority, state.voteCount());
        assertTrue(state.isMajorityGranted());
    }

}
