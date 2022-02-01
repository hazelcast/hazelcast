/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.memory;

/**
 * MemorySize represents a memory size with given value and <code>{@link MemoryUnit}</code>.
 *
 * @see MemoryUnit
 * @see Capacity
 * @since 3.4
 *
 * @deprecated  Since 5.1, {@link Capacity} should be used instead.
 */
public final class MemorySize extends Capacity {

    public MemorySize(long value) {
        super(value, MemoryUnit.BYTES);
    }

    public MemorySize(long value, MemoryUnit unit) {
        super(value, unit);
    }
}
