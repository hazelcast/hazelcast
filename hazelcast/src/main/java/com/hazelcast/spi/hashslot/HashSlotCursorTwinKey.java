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
 * Cursor over assigned slots in a {@link HashSlotArrayTwinKey}. Initially the cursor's location is
 * before the first map entry and the cursor is invalid.
 */
public interface HashSlotCursorTwinKey {
    /**
     * Advances to the next assigned slot.
     * @return true if the cursor advanced. If false is returned, the cursor is now invalid.
     * @throws IllegalStateException if a previous call to advance() already returned false.
     */
    boolean advance();

    /**
     * @return key part 1 of current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key1();

    /**
     * @return key part 2 of current slot.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long key2();

    /**
     * @return Address of the current slot's value block.
     * @throws IllegalStateException if the cursor is invalid.
     */
    long valueAddress();
}
