/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Does the computation needed to transform zero or more input data streams into one
 * output stream. Each input stream corresponds to one incoming edge of the vertex
 * represented by this processor. The edges are identified by their ordinals.
 * <p>
 * If this processor is non-blocking, the processing methods should limit the amount of
 * processing time and data they output per one invocation. A {@code boolean}-returning method
 * ({@link #completeEdge(int)}, {@link #complete()}) should return <code>false</code> to signal
 * it's not done with its work. The method {@link #process(int, Inbox)} should not remove an item
 * from inbox until it is done with it.
 */
public interface Processor {

    /**
     * Initializes this processor with an outbox which will accept its
     * output. This method will be called exactly once and strictly before any
     * calls to processing methods ({@link #process(int, Inbox)}, {@link #completeEdge(int)},
     * {@link #complete()}).
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
     * Called after the edge input with the supplied <code>ordinal</code> is exhausted.
     *
     * @return <code>true</code> if completing this input is now done, <code>false</code> otherwise.
     */
    default boolean completeEdge(int ordinal) {
        return true;
    }

    /**
     * Called after all the inputs are exhausted.
     *
     * @return <code>true</code> if the completing is now done, <code>false</code> otherwise.
     */
    default boolean complete() {
        return true;
    }

    /**
     * Tells whether this processor performs any blocking operations (such as using
     * blocking I/O). By returning <code>false</code> the processor promises not to
     * spend any time waiting for a blocking operation to complete.
     * <p>
     * Depending on the result of this method call, the processor will be driven either
     * by a non-blocking tasklet in a cooperative multithreading scheme, or allocated
     * its own Java thread.
     */
    default boolean isBlocking() {
        return false;
    }
}
