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

package com.hazelcast.jet.internal.api.actor;

import com.hazelcast.jet.api.data.io.ProducerInputStream;

/**
 * Abstract interface fo ringBuffers;
 *
 * @param <T> - type of the content in the ring buffer;
 */
public interface RingBuffer<T> {
    /**
     * Acquire specified amount of elements in buffer;
     *
     * @param acquired - amount of elements to acquire;
     * @return real acquired number of elements;
     */
    int acquire(int acquired);

    /**
     * Write specified amount of elements into the acquired cells;
     *
     * @param chunk    -   chunk with data;
     * @param consumed -   amount of elements to write;
     */
    void commit(ProducerInputStream<T> chunk, int consumed);

    /**
     * Copy data from the buffer into the specified chunk;
     *
     * @param chunk - array buffer;
     * @return -    amount of really read elements;
     */
    int fetch(T[] chunk);

    /**
     * Reset buffer to the initial state
     */
    void reset();
}
