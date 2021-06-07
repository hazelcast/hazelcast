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

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public final class Epoch {

    private static final int ACTIONS_SIZE = 16;

    private static final long FREE = Long.MAX_VALUE;
    private static final long LOCKED = Long.MAX_VALUE - 1;

    private static final AtomicLongFieldUpdater<Epoch> CURRENT = AtomicLongFieldUpdater.newUpdater(Epoch.class, "current");

    private volatile long current;
    private volatile long safe;

    private final AtomicLongArray threadEpochs;

    // TODO: track the actual action count?
    private final Action[] actions = new Action[ACTIONS_SIZE];

    public Epoch(int maxThreads) {
        this.current = 1;
        this.safe = 0;

        this.threadEpochs = new AtomicLongArray(maxThreads);
        for (int i = 0; i < maxThreads; ++i) {
            threadEpochs.set(i, FREE);
        }

        for (int i = 0; i < ACTIONS_SIZE; ++i) {
            actions[i] = new Action();
        }
    }

    /**
     * Registers the current thread identified by the given thread index
     * in this epoch instance.
     * <p>
     * Sets epoch of the current thread to the current global epoch.
     */
    public void register(int threadIndex) {
        assert threadEpochs.get(threadIndex) == FREE;
        threadEpochs.set(threadIndex, current);
    }

    /**
     * Refreshes the epoch of the current thread identified by the given
     * thread index.
     * <p>
     * Sets epoch of the current thread to the current global epoch,
     * then tries to advance the safe epoch while calling epoch actions
     * in unspecified order for the epochs becoming safe.
     */
    public void refresh(int threadIndex) {
        long current = this.current;
        long threadEpoch = threadEpochs.get(threadIndex);
        assert threadEpoch != FREE;
        if (threadEpoch == current) {
            return;
        }

        threadEpochs.set(threadIndex, current);

        long safe = computeSafeEpoch(current);
        updateSafeEpoch(safe);
    }

    /**
     * Bumps the current global epoch and registers the given epoch
     * action for the current epoch (before bumping) on behalf of the
     * current thread identified by the given thread index.
     * <p>
     * Sets epoch of the current thread to the current global epoch
     * (after bumping), then tries to advance the safe epoch while
     * calling epoch actions in unspecified order for the epochs
     * becoming safe.
     */
    public void bump(int threadIndex, Runnable epochAction) {
        long current = CURRENT.incrementAndGet(this);

        while (true) {
            threadEpochs.set(threadIndex, current);
            long safe = computeSafeEpoch(current);
            updateSafeEpoch(safe);

            for (int i = 0; i < ACTIONS_SIZE; ++i) {
                Action action = actions[i];

                if (action.epoch != FREE || !Action.EPOCH.compareAndSet(action, FREE, LOCKED)) {
                    continue;
                }

                action.runnable = epochAction;
                Action.EPOCH.set(action, current - 1);
                return;
            }

            // TODO: spin wait?
            Thread.yield();
            current = this.current;
        }
    }

    /**
     * Unregisters the current thread identified by the given thread
     * index in this epoch instance.
     * <p>
     * Tries to advance the safe epoch while calling epoch actions in
     * unspecified order for the epochs becoming safe.
     */
    public void unregister(int threadIndex) {
        assert threadEpochs.get(threadIndex) != FREE;
        threadEpochs.set(threadIndex, FREE);

        long safe = computeSafeEpoch(current);
        updateSafeEpoch(safe);
    }

    private long computeSafeEpoch(long current) {
        long oldest = current;
        for (int i = 0; i < threadEpochs.length(); ++i) {
            oldest = Math.min(oldest, threadEpochs.get(i));
        }
        return oldest - 1;
    }

    private void updateSafeEpoch(long safe) {
        // Even if some other thread would concurrently update the safe
        // epoch to a more recent epoch than provided to this method,
        // all the matching actions still would be triggered by that
        // other thread and the safe epoch would advance properly on
        // the next iteration (if overwritten by the current thread).

        if (safe <= this.safe) {
            // no change or some other thread advanced the safe epoch
            return;
        }

        for (int i = 0; i < ACTIONS_SIZE; ++i) {
            Action action = actions[i];

            long epoch = action.epoch;
            if (epoch > safe) {
                continue;
            }

            if (!Action.EPOCH.compareAndSet(action, epoch, LOCKED)) {
                continue;
            }

            Runnable runnable = action.runnable;
            action.runnable = null;
            Action.EPOCH.set(action, FREE);

            // If the runnable fails, this epoch instance is left in
            // a consistent state. The remaining actions would be
            // invoked on the next attempt to advance the safe epoch.
            runnable.run();
        }

        this.safe = safe;
    }

    private static final class Action {

        static final AtomicLongFieldUpdater<Action> EPOCH = AtomicLongFieldUpdater.newUpdater(Action.class, "epoch");

        volatile long epoch = FREE;

        Runnable runnable;

    }

}
