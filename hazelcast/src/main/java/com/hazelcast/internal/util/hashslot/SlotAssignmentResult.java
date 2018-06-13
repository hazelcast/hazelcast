/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.hashslot;


/**
 * <p>
 * A <i>Flyweight</i> object that carries information about the result of
 * slot assignment.
 * This includes the address of the block and if the block is newly assigned
 * or if there was an existing block for the required key.
 * </p><p>
 * <b>Since this is a <i>Flyweight</i>-style object, the same instance is used
 * for many assignment invocations and a single object is valid only up to the
 * next assignment operation</b>.
 * </p>
 *
 * @see HashSlotArray8byteKey#ensure(long)
 * @see HashSlotArray12byteKey#ensure(long, int)
 * @see HashSlotArray16byteKey#ensure(long, long)
 */
public interface SlotAssignmentResult {

    /**
     * @return current slot address of this flyweight
     */
    long address();

    /**
     * @return {@code true} if a new slot had to be assigned in the last
     * assignment invocation, {@code false} otherwise.
     */
    boolean isNew();
}
