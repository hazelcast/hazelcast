/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * An object that carries information about the result of a slot assignment
 * invocation.
 * The returned object contains the slot value block address and whether a new
 * slot had to be assigned. Each hash slot array implementation keeps a reference
 * to the returned object and will always return the same instance, albeit with
 * updated fields on each new invocation.
 * This means the returned object is valid until the next invocation of this
 * method.
 *
 * @see HashSlotArray8byteKey#ensure(long)
 * @see HashSlotArray12byteKey#ensure(long, int)
 * @see HashSlotArray16byteKey#ensure(long, long)
 */
public interface SlotAssignmentResult {

    /**
     * @return slot address of the latest assignment invocation
     */
    long address();

    /**
     * @return {@code true} if a new slot had to be assigned in the last
     * assignment invocation, {@code false} otherwise.
     */
    boolean isNew();
}
