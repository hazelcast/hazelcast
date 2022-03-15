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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.GlobalMemoryAccessor;

import static com.hazelcast.internal.memory.impl.AlignmentUtil.IS_PLATFORM_BIG_ENDIAN;

/**
 * Base class for {@link sun.misc.Unsafe} backed {@link GlobalMemoryAccessor} implementations.
 */
abstract class UnsafeBasedMemoryAccessor implements GlobalMemoryAccessor {

    /**
     * Returns whether memory accessors of type {@link UnsafeBasedMemoryAccessor} are available or not.
     */
    public static boolean isAvailable() {
        return UnsafeUtil.UNSAFE_AVAILABLE;
    }

    @Override
    public boolean isBigEndian() {
        return IS_PLATFORM_BIG_ENDIAN;
    }
}
