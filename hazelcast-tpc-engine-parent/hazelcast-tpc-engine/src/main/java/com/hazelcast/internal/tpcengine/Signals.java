/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * Contains signals (tasks) that are send outside of the reactor thread and
 * require the attention of the reactor. The reactor will periodically check if
 * there are any raised signals and process them.
 * <p>
 * Signals are not like Linux Signals whereby there are some well known signals
 * like division by zero and handlers for each of the signals.
 * <p/>
 * The 2 primary sources of signals for now are the AsyncSocket and the TaskQueue.
 */
public final class Signals {

    private final Reactor reactor;
    private final MpscArrayQueue<Runnable> queue;
    private final CircularQueue<Runnable> tmp;
    private final MessagePassingQueue.Consumer<Runnable> drainToTmp;

    public Signals(Reactor reactor, int capacity) {
        this.reactor = checkNotNull(reactor, "reactor");
        this.queue = new MpscArrayQueue<>(capacity);
        this.tmp = new CircularQueue<>(capacity);
        this.drainToTmp = (action) -> tmp.add(action);
    }

    /**
     * Processes all raises signals.
     *
     * Should only be called from the eventloop-thread.
     */
    public void process() {
        // The queue is first drained to the tmp so that we get
        // the least amount of cache trashing on the queue instead
        // of polling and processing an item at a time.
        queue.drain(drainToTmp);

        // And now we process the tmp
        final CircularQueue<Runnable> tmp = this.tmp;
        for (; ; ) {
            Runnable action = tmp.poll();
            if (action == null) {
                break;
            }
            action.run();
        }
    }

    /**
     * Raises a signal and wakes up the reactor.
     *
     * @param action the action
     * @throws NullPointerException  if action is null
     * @throws IllegalStateException when there isn't enough space in the queue.
     */
    public void raise(Runnable action) {
        if (!queue.offer(action)) {
            throw new IllegalStateException("Not enough space");
        }
        reactor.wakeup();
    }

    /**
     * Checks if there are raised signals.
     *
     * @return true if there are raised signals, false otherwise.
     */
    public boolean hasRaised() {
        return !queue.isEmpty();
    }
}
