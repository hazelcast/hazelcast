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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StateMachineTest {

    private enum State {
        A, B, C
    }

    private StateMachine<State> machine = StateMachine.of(State.A).withTransition(State.A, State.B)
            .withTransition(State.B, State.C);

    @Test
    public void testIsInInitialState_whenCreated() {
        assertTrue(machine.is(State.A));
    }

    @Test
    public void testChangesState_whenTransitionValid() {
        machine.next(State.B);
        assertTrue(machine.is(State.B));

        machine.next(State.C);
        assertTrue(machine.is(State.C));
    }

    @Test(expected = IllegalStateException.class)
    public void testThrowsException_whenTransitionInvalid() {
        machine.next(State.C);
    }

    @Test
    public void testStaysAtState_whenAlreadyThere() {
        machine.nextOrStay(State.A);
        assertTrue(machine.is(State.A));
    }
}
