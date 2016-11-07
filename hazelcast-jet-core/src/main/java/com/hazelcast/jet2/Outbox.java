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

/**
 * Single-threaded object which acts as a data sink for a {@link Processor}. While handling
 * an invocation of {@link Processor#process(int, Inbox)} the processor must deliver all the
 * output data, separated by destination edge, into the outbox by calling {@link #add(int, Object)}
 * or the convenience method {@link #add(Object)}. After each invocation of
 * {@link Processor#process(int, Inbox)} the execution engine will try to flush the outbox into
 * concurrent queues and/or network pipes.
 * <p>
 * Although not strictly required, the processor is advised to check {@link #isHighWater(int)}
 * regularly and refrain from outputting more data if it returns true.
 */
public interface Outbox {

    /**
     * Convenience for {@code add(item, 0)}.
     */
    void add(Object item);

    /**
     * Adds an item addressed at edge with given ordinal to this outbox.
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
