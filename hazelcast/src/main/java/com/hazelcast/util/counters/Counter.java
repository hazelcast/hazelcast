/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.counters;

/**
 * A Counter keeps track of a long value.
 *
 * It depends on the counter if increments are thread-safe.
 *
 * The get is thread-safe in the sense that it will see a recently published value. It doesn't mean that it
 * will see the most recently published value.
 */
public interface Counter {

    /**
     * Gets the current value of the counter.
     *
     * @return the current value of the counter.
     */
    long get();

    /**
     * Increments the counter by one.
     */
    void inc();

    /**
     * Increments (or decrements) the counter by the given amount.
     *
     * If the amount is negative, the counter is decremented.
     *
     * @param amount the amount to increase or decrease the counter with.
     */
    void inc(long amount);
}
