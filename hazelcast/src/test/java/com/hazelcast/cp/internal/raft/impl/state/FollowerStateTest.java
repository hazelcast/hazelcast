/*
 * Original work Copyright (c) 2020, MicroRaft.
 * Modified work Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.impl.state.FollowerState.MIN_BACKOFF_ROUNDS;
import static com.hazelcast.cp.internal.raft.impl.state.FollowerState.MAX_BACKOFF_ROUND;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FollowerStateTest {

    private final FollowerState followerState = new FollowerState(0, 1);

    @Test
    public void testFirstBackoffRound() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();

        assertThat(flowControlSeqNum).isEqualTo(1);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(MIN_BACKOFF_ROUNDS);
    }

    @Test
    public void testValidFlowControlResponseReceivedOnFirstBackoff() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean success = followerState.appendRequestAckReceived(flowControlSeqNum);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    // TODO RU_COMPAT_5_3 test Version 5.3 compatibility. Test should be removed at Version 5.5
    @Test
    public void compatibilityTestBackoffIsRetestedIfFlowControlIsMinusOne() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean success = followerState.appendRequestAckReceived(-1);

        assertThat(success).isTrue();
        assertThat(flowControlSeqNum).isGreaterThanOrEqualTo(1);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidFlowControlResponseReceivedOnFirstBackoff() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean success = followerState.appendRequestAckReceived(flowControlSeqNum + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(MIN_BACKOFF_ROUNDS);
    }

    @Test
    public void testUncompletedFirstBackoff() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean backoffCompleted = followerState.completeAppendRequestBackoffRound();

        assertThat(backoffCompleted).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(MIN_BACKOFF_ROUNDS - 1);
    }

    @Test
    public void testCompletedFirstBackoff() {
        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean backoffCompleted = executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        assertThat(backoffCompleted).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterBackoffIsSetForSecondRequest() {
        long flowControlSeqNum1 = followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum2 = followerState.setAppendRequestBackoff();
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);

        boolean success = followerState.appendRequestAckReceived(flowControlSeqNum1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum2);
        assertThat(followerState.backoffRound()).isEqualTo(2 * MIN_BACKOFF_ROUNDS);
    }

    @Test
    public void testValidFlowControlResponseReceivedOnSecondBackoff() {
        followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean success = followerState.appendRequestAckReceived(flowControlSeqNum);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testCompletedSecondBackoff() {
        followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum = followerState.setAppendRequestBackoff();
        boolean backoffCompleted1 = followerState.completeAppendRequestBackoffRound();

        assertThat(backoffCompleted1).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo((2 * MIN_BACKOFF_ROUNDS) - 1);

        boolean backoffCompleted2 = executeCompleteAppendReqBackoffRound((2 * MIN_BACKOFF_ROUNDS) - 1);

        assertThat(backoffCompleted2).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testSecondResponseReceivedAfterBackoffIsSetForSecondRequest() {
        long flowControlSeqNum1 = followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum2 = followerState.setAppendRequestBackoff();
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);

        boolean success = followerState.appendRequestAckReceived(flowControlSeqNum1);
        assertThat(success).isFalse();
        assertThat(followerState.backoffRound()).isEqualTo(2 * MIN_BACKOFF_ROUNDS);

        followerState.completeAppendRequestBackoffRound();
        success = followerState.appendRequestAckReceived(flowControlSeqNum2);

        assertThat(success).isTrue();
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testMaxBackoff() {
        long flowControlSeqNum1 = followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum2 = followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(2 * MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum3 = followerState.setAppendRequestBackoff();
        executeCompleteAppendReqBackoffRound(4 * MIN_BACKOFF_ROUNDS);

        long flowControlSeqNum4 = followerState.setAppendRequestBackoff();

        assertThat(flowControlSeqNum4).isGreaterThan(flowControlSeqNum3);
        assertThat(flowControlSeqNum3).isGreaterThan(flowControlSeqNum2);
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum4);
        assertThat(followerState.backoffRound()).isEqualTo(MAX_BACKOFF_ROUND);
    }

    @Test
    public void testNoBackoffOverflow() {
        for (int i = 0; i < 64; i++) {
            followerState.setAppendRequestBackoff();
            while (followerState.isAppendRequestBackoffSet()) {
                followerState.completeAppendRequestBackoffRound();
            }

        }
    }

    private boolean executeCompleteAppendReqBackoffRound(int times) {
        boolean res = false;
        for (int i = 0; i < times; i++) {
            res = followerState.completeAppendRequestBackoffRound();
        }
        return res;
    }

}
