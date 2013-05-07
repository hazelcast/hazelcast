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

package com.hazelcast.map;

import java.io.Serializable;
import java.util.Map;

public interface EntryProcessor<K, V> extends Serializable {

    /**
     * Process the entry without worrying about concurrency.
     * <p/>
     *
     * @param entry entry to be processes
     * @return result of the process
     */
    Object process(Map.Entry<K, V> entry);

    /**
     * Get the entry processor to be applied to backup entries.
     * <p/>
     *
     * @return back up processor
     */
    EntryBackupProcessor<K, V> getBackupProcessor();
}
