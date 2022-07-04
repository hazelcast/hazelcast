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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.util.ProgressState;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

/**
 * The inbound side of a data stream corresponding to a single DAG edge identified by its ordinal. In the
 * {@code ProcessorTasklet} it corresponds to the target of an edge; in {@code SenderTasklet} it corresponds to the
 * origin of an edge.
 */
public interface InboundEdgeStream {

    /**
     * Returns the represented edge's ordinal.
     */
    int ordinal();

    /**
     * Returns the represented edge's priority
     */
    int priority();

    /**
     * Passes the items from the queues to the predicate while it returns {@code true}.
     */
    @Nonnull
    ProgressState drainTo(@Nonnull Predicate<Object> dest);

    /**
     * Returns true after all the input queues are done.
     */
    boolean isDone();

    /**
     * Returns the total capacity of input queues.
     */
    int capacities();

    /**
     * Returns the total number of items in input queues.
     */
    int sizes();

    /**
     * Returns the top WM observed on any of the input queues.
     */
    long topObservedWm();

    /**
     * Returns the last coalesced WM that was forwarded from the edge.
     */
    long coalescedWm();
}
