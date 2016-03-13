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

package com.hazelcast.spi.hashslot;

/**
 * Cursor over assigned hash slots in a {@link HashSlotArray}.
 * Initially the cursor's location is before the first slot and the cursor is invalid.
 */
public interface HashSlotCursor {
    /**
     * Resets the cursor to the initial state.
     */
    void reset();

    /**
     * Advance to the next assigned slot.
     * @return {@code true} if the cursor advanced. If {@code false} is returned, the cursor is now invalid.
     * @throws IllegalStateException if a previous call to advance() already returned false.
     */
    boolean advance();

    /**
     * @return the key of the current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key();

    /**
     * @return Address of the current slot's value block.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long valueAddress();
}
