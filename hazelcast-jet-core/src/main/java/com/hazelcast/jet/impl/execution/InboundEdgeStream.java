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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.util.ProgressState;

import java.util.Collection;

/**
 * The inbound side of a data stream corresponding to a single DAG edge identified by its ordinal. In the
 * {@code ProcessorTasklet} it corresponds to the target of an edge; in {@code SenderTasklet} it corresponds to the
 * origin of an edge.
 */
public interface InboundEdgeStream {

    int ordinal();

    int priority();

    /**
     * Drains all currently available items to the supplied destination collection.
     * The two {@code boolean} components of the return value have the following meaning:
     * <ul><li>
     *   {@link ProgressState#isMadeProgress()}: whether any items were drained from the stream.
     * </li><li>
     *   {@link ProgressState#isDone()}: whether the stream is exhausted and no more items will arrive.
     * </li></ul>
     */
    ProgressState drainTo(Collection<Object> dest);
}
