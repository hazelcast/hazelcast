/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import javax.annotation.Nonnull;

/**
 * Does the computation needed to transform zero or more input data streams into
 * zero or more output streams. Each input/output stream corresponds to one edge
 * of the vertex represented by this processor. The correspondence between a stream
 * and an edge is established via the edge's <em>ordinal</em>.
 * <p>
 * The special case of zero input streams applies to a <em>source</em> vertex, which
 * gets its data from the environment. The special case of zero output streams applies
 * to a <em>sink</em> vertex, which pushes its data to the environment.
 * <p>
 * The processor accepts input from instances of {@link Inbox} and pushes its output
 * to an instance of {@link Outbox}.
 * <p>
 * The processing methods should limit the amount of data they output per invocation
 * because the outbox will not be emptied until the processor yields control back to
 * its caller. Specifically, {@code Outbox} has a method {@link Outbox#isHighWater isHighWater()}
 * that can be tested to see whether it's time to stop pushing more data into it.  There is
 * also a finer-grained method {@link Outbox#isHighWater(int) isHighWater(ordinal)}, which
 * tells the state of an individual output bucket.
 * <p>
 * If this processor declares itself as "cooperative" ({@link #isCooperative()} returns
 * {@code true}), it should also limit the amount of time it spends per call because it
 * will participate in a cooperative multithreading scheme.
 */
public interface Processor {

    /**
     * Initializes this processor with the outbox that the processing methods
     * must use to deposit their output items. This method will be called exactly
     * once and strictly before any calls to processing methods ({@link #process(int, Inbox)},
     * {@link #completeEdge(int)}, {@link #complete()}).
     */
    void init(@Nonnull Outbox outbox);

    /**
     * Processes some items in the supplied inbox. Removes the items it's done with.
     * Does not remove an item until it is done with it.
     *
     * @param ordinal ordinal of the edge the item comes from
     * @param inbox   the inbox containing the pending items
     */
    void process(int ordinal, @Nonnull Inbox inbox);

    /**
     * Called after the edge input with the supplied {@code ordinal} is exhausted. If
     * it returns {@code false}, it will be invoked again until it returns {@code true},
     * and until it does, no other methods will be invoked on the processor.
     *
     * @return {@code true} if the processor is now done completing this input,
     * {@code false} otherwise.
     */
    default boolean completeEdge(int ordinal) {
        return true;
    }

    /**
     * Called after all the inputs are exhausted. If it returns {@code false}, it will be
     * invoked again until it returns {@code true}. After this method is called, no other
     * processing methods will be called on this processor.
     *
     * @return {@code true} if the completing step is now done, {@code false} otherwise.
     */
    default boolean complete() {
        return true;
    }

    /**
     * Tells whether this processor is able to participate in cooperative multithreading.
     * This means that each invocation of a processing method will take a reasonably small
     * amount of time (up to a millisecond). A cooperative processor should not attempt
     * any blocking I/O operations.
     * <p>
     * If this processor declares itself non-cooperative, it will be allocated a dedicated
     * Java thread. Otherwise it will be allocated a tasklet which shares a thread with other
     * tasklets.
     */
    default boolean isCooperative() {
        return true;
    }
}
