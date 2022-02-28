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

package com.hazelcast.internal.memory;

/**
 * Garbage collector statistics for the JVM which current HazelcastInstance
 * belongs to.
 * <p>
 * Shows major/minor collection counts and consumed CPU times.
 *
 * @see com.hazelcast.internal.memory.MemoryStats
 */
public interface GarbageCollectorStats {

    /**
     * Returns major collection count.
     * <p>
     * Major collection is identified by matching heap memory region name
     * with known names. If a collection cannot be matched then it's reported
     * as unknown count.
     *
     * @return major collection count.
     * @see #getUnknownCollectionCount()
     */
    long getMajorCollectionCount();

    /**
     * Returns total major collection time in ms.
     * <p>
     * Major collection is identified by matching heap memory region name
     * with known names. If a collection cannot be matched then it's reported
     * as unknown count.
     *
     * @return total major collection count in ms.
     * @see #getUnknownCollectionTime()
     */
    long getMajorCollectionTime();

    /**
     * Returns minor collection count.
     * <p>
     * Minor collection is identified by matching heap memory region name
     * with known names. If a collection cannot be matched then it's reported
     * as unknown count.
     *
     * @return minor collection count.
     * @see #getUnknownCollectionCount()
     */
    long getMinorCollectionCount();

    /**
     * Returns total minor collection time in ms.
     * <p>
     * Minor collection is identified by matching heap memory region name
     * with known names. If a collection cannot be matched then it's reported
     * as unknown count.
     *
     * @return total minor collection count in ms.
     * @see #getUnknownCollectionTime()
     */
    long getMinorCollectionTime();

    /**
     * Returns collection count which cannot be identified as major or minor.
     *
     * @return unidentified collection count.
     */
    long getUnknownCollectionCount();

    /**
     * Returns total collection time which cannot be identified as major or minor.
     *
     * @return total unidentified collection time in ms.
     */
    long getUnknownCollectionTime();
}
