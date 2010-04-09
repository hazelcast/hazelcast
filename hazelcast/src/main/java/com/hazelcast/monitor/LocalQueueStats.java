/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor;

/**
 * Local queue statistics.
 * 
 */
public interface LocalQueueStats {
    /**
     * Returns the number of owned items in this member.
     *
     * @return number of owned items.
     */
    int getOwnedItemCount();

    /**
     * Returns the number of backup items in this member.
     *
     * @return number of backup items.
     */
    int getBackupItemCount();

    /**
     * Returns the min age of the items in this member.
     *
     * @return min age
     */
    long getMinAge();

    /**
     * Returns the
     * @return
     */
    long getMaxAge();

    long getAveAge();

    LocalQueueOperationStats getQueueOperationStats();
}
