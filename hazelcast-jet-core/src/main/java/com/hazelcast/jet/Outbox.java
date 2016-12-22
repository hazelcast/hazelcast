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

/**
 * Single-threaded object which acts as a data sink for a {@link Processor}. The outbox
 * consists of individual output buckets, one per outbound edge of the vertex represented
 * by the associated processor. The processor must deliver its output items,
 * separated by destination edge, into the outbox by calling {@link #add(int, Object)}
 * or {@link #add(Object)}.
 * <p>
 * The execution engine will not try to flush the outbox into downstream queues until the
 * processing method returns. Therefore the processor is advised to check {@link #isHighWater(int)}
 * regularly and refrain from outputting more data when it returns true.
 */
public interface Outbox {

    /**
     * Adds the supplied item to all the output buckets.
     */
    void add(Object item);

    /**
     * Adds the supplied item to the output bucket with the supplied ordinal.
     */
    void add(int ordinal, Object item);

    /**
     * Returns {@code true} if {@link #isHighWater(int)} would return true for any legal
     * value of {@code ordinal}.
     */
    boolean isHighWater();

    /**
     * Returns {@code true} if no more data should be added to this outbox during this invocation
     * of {@link Processor#process(int, Inbox)}.
     */
    boolean isHighWater(int ordinal);
}
