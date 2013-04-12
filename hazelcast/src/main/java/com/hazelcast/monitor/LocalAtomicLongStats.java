/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

public interface LocalAtomicLongStats extends LocalInstanceStats {
    /**
     * Returns the creation time of this atomic long on this member.
     *
     * @return creation time of this atomic long on this member.
     */
    long getCreationTime();

    /**
     * Returns the last access time of the atomic long.
     *
     * @return last access time.
     */
    long getLastAccessTime();

    /**
     * Returns the last update time of the  atomic long.
     *
     * @return last update time.
     */
    long getLastUpdateTime();

    /**
     //     * Returns the number of operations that modified the stored atomic value.
     //     *
     //     * @return number of modified operations
     //     */
//    public long getNumberOfModifyOps();
//
//    /**
//     * Returns the number of operations that did not modify the stored atomic value.
//     *
//     * @return number of non-modified operations
//     */
//    public long getNumberOfNonModifyOps();
}
