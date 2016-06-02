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

package com.hazelcast.jet.api.counters;

import java.io.Serializable;

/**
 * Represents accumulator;
 *
 * @param <V> - type of the input value;
 * @param <R> - type of the output value;
 */
public interface Accumulator<V, R extends Serializable> extends Serializable, Cloneable {
    /**
     * @param value The value to add to the accumulator
     */
    void add(V value);

    /**
     * @return local The local value
     */
    R getLocalValue();

    /**
     * Reset the local value. This only affects the current UDF context.
     */
    void resetLocal();

    /**
     * Used by system internally to merge the collected parts of an accumulator
     * at the end of the job.
     *
     * @param other Reference to accumulator to merge in.
     */
    void merge(Accumulator<V, R> other);
}
