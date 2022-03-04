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

package com.hazelcast.internal.util;

/**
 * Used for providing runtime memory information of Java virtual machine.
 *
 * @see java.lang.Runtime
 * @see java.lang.management.ManagementFactory#getMemoryMXBean()
 */
public interface MemoryInfoAccessor {

    /**
     * Returns total amount allocated memory in the Java virtual machine in bytes.
     *
     * @return total memory allocated in bytes.
     *
     * @see Runtime#totalMemory()
     */
    long getTotalMemory();

    /**
     * Returns the amount of free memory in the Java virtual machine in bytes.
     *
     * @return free memory in bytes
     *
     * @see Runtime#freeMemory()
     */
    long getFreeMemory();

    /**
     * Returns the maximum amount of memory that the Java virtual machine will attempt to use in bytes.
     *
     * @return maximum memory in bytes.
     *
     * @see Runtime#maxMemory()
     */
    long getMaxMemory();
}
