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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.internal.util.MemoryInfoAccessor;

/**
 * Returns zero for every method call.
 * Used for testing purposes.
 */
public class ZeroMemoryInfoAccessor implements MemoryInfoAccessor {

    @Override
    public long getTotalMemory() {
        return 0;
    }

    @Override
    public long getFreeMemory() {
        return 0;
    }

    @Override
    public long getMaxMemory() {
        return 0;
    }
}
