/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tstore;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class State {

    private static final int PHASE_BITS = 3;
    private static final long PHASE_MASK = (1L << PHASE_BITS) - 1;
    private static final long INTERMEDIATE_MASK = 1L << (PHASE_BITS - 1);
    private static final int VERSION_SHIFT = PHASE_BITS;

    public static final long PHASE_REST = 0;
    public static final long PHASE_PREPARE_GROW = 1;
    public static final long PHASE_GROWING = 2;

    public static final long DETACHED = make(PHASE_REST, 0);
    public static final long INITIAL = make(PHASE_REST, 1);

    public static long phase(long state) {
        return state & PHASE_MASK;
    }

    public static long version(long state) {
        return state >>> VERSION_SHIFT;
    }

    public static long make(long phase, long version) {
        return phase | version << VERSION_SHIFT;
    }

    public static long stable(long state) {
        return state & ~INTERMEDIATE_MASK;
    }

    public static long intermediate(long state) {
        return state | INTERMEDIATE_MASK;
    }

    public interface Listener {

        void globalEntering(long next, long current);

        void globalEntered(long current, long previous);

        void threadEntering(long next, long current);

        void threadEntered(long current, long previous);

    }

    public static abstract class Machine {

        private final Listener[] listeners;

        public Machine(State.Listener... listeners) {
            this.listeners = listeners;
        }

        /**
         * Returns the next state of this machine given its current state.
         * <p>
         * This method should NOT have any side effects, it should be
         * a pure stateless function.
         */
        public abstract long nextState(long current);

        /**
         * Returns a rest state right before this machine first non-rest
         * state given its current state.
         * <p>
         * This method should NOT have any side effects, it should be
         * a pure stateless function.
         */
        public abstract long cycleStart(long current);

        public void globalEntering(long next, long current) {
            for (State.Listener listener : listeners) {
                listener.globalEntering(next, current);
            }
        }

        public void globalEntered(long current, long previous) {
            for (State.Listener listener : listeners) {
                listener.globalEntered(current, previous);
            }
        }

        public void threadEntering(long next, long current) {
            for (State.Listener listener : listeners) {
                listener.threadEntering(next, current);
            }
        }

        public void threadEntered(long current, long previous) {
            for (State.Listener listener : listeners) {
                listener.threadEntered(current, previous);
            }
        }

    }

    private static final AtomicLongFieldUpdater<State> STATE = AtomicLongFieldUpdater.newUpdater(State.class, "state");
    private static final AtomicIntegerFieldUpdater<State> ACTIVE = AtomicIntegerFieldUpdater.newUpdater(State.class, "active");

    private final AtomicLongArray threadStates;

    private volatile long state = INITIAL;
    private volatile int active;
    private volatile State.Machine machine = NullStateMachine.INSTANCE;

    public State(int maxThreads) {
        this.threadStates = new AtomicLongArray(maxThreads);
    }

    /**
     * Registers the current thread identified by its thread index in
     * this state instance.
     */
    public long acquire(int threadIndex) {
        // Determine the current state and machine (machine is acting
        // as a validation stamp).

        State.Machine machine = this.machine;
        long state = this.state;
        State.Machine freshMachine;
        while (machine != (freshMachine = this.machine)) {
            machine = freshMachine;
            state = this.state;
        }
        state = stable(state);

        if (phase(state) == PHASE_REST) {
            // the current machine reached its end or isn't started yet
            threadStates.set(threadIndex, state);
            return state;
        }

        // Step the thread until the current global state.

        long current = machine.cycleStart(state);
        assert current == stable(current) && phase(current) == PHASE_REST;
        long next;
        do {
            next = machine.nextState(current);

            machine.threadEntering(next, current);
            threadStates.set(threadIndex, next);
            machine.threadEntered(next, current);

            current = next;
        } while (next != state);

        return state;
    }

    /**
     * Starts the given state machine on this state instance.
     */
    public boolean start(int threadIndex, State.Machine machine) {
        if (!ACTIVE.compareAndSet(this, 0, 1)) {
            // some other state machine is active
            return false;
        }

        // The current thread is the only one in this method (for this
        // state instance) at this point.

        // The current stale (yes, stale) machine is kind of active now,
        // but no threads can step it: the stale machine already reached
        // its end phase (REST) and transitions from REST to any other
        // phase are allowed only in this method.

        // Compute the new state.

        long oldState = this.state;
        assert phase(oldState) == PHASE_REST;
        long newState = machine.nextState(oldState);
        assert newState == stable(newState) && version(newState) >= version(oldState);
        if (phase(newState) == PHASE_REST) {
            // machine decided to abort itself
            active = 0;
            return false;
        }

        // Expose the machine. Threads observing the new machine and the
        // old state (REST) are unable to step it: transitions from REST
        // to any other phase are allowed only in this method and the
        // current thread is the only one here.

        this.machine = machine;

        // Start the machine.

        machine.globalEntering(newState, oldState);
        this.state = newState;
        machine.globalEntered(newState, oldState);

        long threadState = threadStates.get(threadIndex);
        assert threadState == stable(threadState) && threadState != DETACHED;
        machine.threadEntering(newState, threadState);
        threadStates.set(threadIndex, newState);
        machine.threadEntered(newState, threadState);

        return true;
    }

    /**
     * Steps the active state machine from the given expected state to
     * the next one provided by the machine.
     */
    public void step(int threadIndex, long expected) {
        assert expected == stable(expected) && expected != PHASE_REST;

        if (!STATE.compareAndSet(this, expected, intermediate(expected))) {
            // some other thread has changed the state
            return;
        }

        // At this point the state is marked as intermediate, so no other
        // threads can step the machine.

        State.Machine machine = this.machine;
        long newState = machine.nextState(expected);
        machine.globalEntering(newState, expected);
        this.state = newState;
        machine.globalEntered(newState, expected);

        long threadState = threadStates.get(threadIndex);
        assert threadState == stable(threadState) && threadState != DETACHED;
        machine.threadEntering(newState, threadState);
        threadStates.set(threadIndex, newState);
        machine.threadEntered(newState, threadState);

        if (phase(newState) == PHASE_REST) {
            // machine has ended
            this.active = 0;
        }
    }

    /**
     * Refreshes the state as seen by the current thread identified by
     * the given thread index.
     */
    public long refresh(int threadIndex) {
        // Determine the current state and machine (machine is acting
        // as a validation stamp).

        State.Machine machine = this.machine;
        long state = this.state;
        State.Machine freshMachine;
        while (machine != (freshMachine = this.machine)) {
            machine = freshMachine;
            state = this.state;
        }
        state = stable(state);

        // TODO: Proper stepping of the thread through all the states
        //       until the current one.

        // Refresh.

        long threadState = threadStates.get(threadIndex);
        machine.threadEntering(state, threadState);
        threadStates.set(threadIndex, state);
        machine.threadEntered(state, threadState);
        return state;
    }

    /**
     * Unregisters the current thread identified by the given thread index
     * in this state instance.
     */
    public void release(int threadIndex) {
        // TODO: Semantics of this method? Threads leaving in the middle
        //       of the state machine cycle feel awkward.

        State.Machine machine = this.machine;
        long state = threadStates.get(threadIndex);
        assert state == stable(state) && state != DETACHED;

        machine.threadEntering(DETACHED, state);
        threadStates.set(threadIndex, DETACHED);
        machine.threadEntered(DETACHED, state);
    }

    private static final class NullStateMachine extends State.Machine {

        public static final NullStateMachine INSTANCE = new NullStateMachine();

        @Override
        public long nextState(long current) {
            assert false;
            return 0;
        }

        @Override
        public long cycleStart(long current) {
            assert false;
            return 0;
        }

    }

}
