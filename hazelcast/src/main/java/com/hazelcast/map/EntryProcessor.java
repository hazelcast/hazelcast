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

/**
 * An EntryProcessor passes you a {@link java.util.Map.Entry}. At the time you receive it
 * the entry is locked and not released until the EntryProcessor completes.
 * This obviates the need to explicitly lock as would be required with a {@link java.util.concurrent.ExecutorService}.
 * <p/>
 * Performance can be very high as the data is not moved off the Member partition. This avoids network cost and, if
 * the storage format is {@link com.hazelcast.config.InMemoryFormat#OBJECT} then there is no de-serialization or serialization
 * cost.
 * <p/>
 * EntryProcessors execute on the partition thread in a member. Multiple operations on the same partition are queued.
 * <p/>
 * While executing partition migrations are not allowed. Any migrations are queued on the partition thread.
 * <p/>
 * An EntryProcessor may not be re-entrant i.e. it may not access the same {@link Map}. Limitation: you can only access
 * data on the same partition.
 * <p/>
 * Note that to modify an entry by using EntryProcessors you should explicitly call
 * {@link java.util.Map.Entry#setValue} method of {@link java.util.Map.Entry} such as:
 * <p/>
 * <pre>
 * <code>
 * {@literal@}Override
 *     public Object process(Map.Entry entry) {
 *        Value value = entry.getValue();
 *        // process and modify value
 *        // ...
 *        entry.setValue(value);
 *        return result;
 *    }
 * </code>
 * </pre>
 * otherwise EntryProcessor does not guarantee to modify the entry.
 *
 * @param <K>
 * @param <V>
 */
public interface EntryProcessor<K, V> extends Serializable {

    /**
     * Process the entry without worrying about concurrency.
     * <p/>
     * Note that to modify an entry by using EntryProcessor you should explicitly call
     * {@link java.util.Map.Entry#setValue} method of {@link java.util.Map.Entry} such as:
     * <p/>
     * <pre>
     * <code>
     * {@literal@}Override
     *        public Object process(Map.Entry entry) {
     *          Value value = entry.getValue();
     *          // process and modify value
     *          // ...
     *          entry.setValue(value);
     *          return result;
     *        }
     * </code>
     * </pre>
     * otherwise EntryProcessor does not guarantee to modify the entry.
     *
     * @param entry entry to be processed
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
