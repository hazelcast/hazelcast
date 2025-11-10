/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

/**
 * Cursor over a collection of {@code long} values. Initially the cursor's location is
 * before the first element and the cursor is invalid. The cursor becomes invalid again after a
 * call to {@link #advance()} returns {@code false}. After {@link #advance()} returns {@code false},
 * it is illegal to call any methods except {@link #reset}.
 */
public interface LongCursor {

    /**
     * Advances to the next {@code long} element. It is illegal to call this method after a previous
     * call returned {@code false}. An {@code AssertionError} may be thrown.
     *
     * @return {@code true} if the cursor advanced. If {@code false} is returned, the cursor is now invalid.
     */
    boolean advance();

    /**
     * @return the {@code long} value at the cursor's current position
     */
    long value();

    /**
     * Resets the cursor to the initial state.
     */
    void reset();
}
