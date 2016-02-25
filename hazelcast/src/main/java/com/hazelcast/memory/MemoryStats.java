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

package com.hazelcast.memory;

/**
 * Memory statistics for abstract MemoryManager
 */
public interface MemoryStats {
    /**
     * @return - total memory available for MemoryManager
     */
    long getTotal();

    /**
     * @return - amount of free memory to allocate
     */
    long getFree();

    /**
     * @return - maximal amount of memory which can be allocated
     */
    long getMax();

    /**
     * @return -amount of memory which has been commited
     */
    long getCommitted();

    /**
     * @return - amount of memory which has been used
     */
    long getUsed();
}
