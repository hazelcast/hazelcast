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

package com.hazelcast.jet2;

import javax.annotation.Nonnull;

/**
 * Does the computation needed to transform zero or more named input data streams into one
 * output stream.
 * <p/>
 * The processing methods should limit the amount of processing time and data they output per
 * one invocation. A method should return <code>false</code> to signal it's not done with the
 * current step/item. When the caller is ready to invoke the method again, it will invoke it with
 * the same arguments as the previous time.
 */
public interface Processor {

    /**
     * Initialize the processor with the {@link ProcessorContext} and an {@link Outbox} that
     * can accept processing results. This method will be called exactly once and strictly before any
     * calls to {@link #process(int, Object)} or {@link #complete(int)}.
     */
    void init(@Nonnull ProcessorContext context, @Nonnull Outbox outbox);

    /**
     * Processes the supplied input item
     *
     * @param ordinal ordinal of the input where the item originates from
     * @param item    item to be processed
     * @return <code>true</code> if this item has now been processed, <code>false</code> otherwise.
     */
    boolean process(int ordinal, Object item);

    /**
     * Called after the input with the supplied <code>ordinal</code> is exhausted.
     *
     * @return <code>true</code> if completing this input is now done, <code>false</code> otherwise.
     */
    boolean complete(int ordinal);

    /**
     * Called after all the inputs are exhausted.
     *
     * @return <code>true</code> if the completing is now done, <code>false</code> otherwise.
     */
    boolean complete();

    /**
     * Tells whether this processor performs any blocking operations
     * (such as using a blocking I/O). By returning <code>false</code> the processor promises
     * not to spend any time waiting for a blocking operation to complete.
     */
    default boolean isBlocking() {
        return false;
    }
}
