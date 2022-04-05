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

package com.hazelcast.cache;

import javax.cache.processor.EntryProcessor;

/**
 * An invocable function that allows applications to perform compound operations
 * on a {@link javax.cache.Cache.Entry} atomically, according to the defined
 * consistency of a {@link javax.cache.Cache}.
 * <p>
 * The difference from the normal {@link javax.cache.processor.EntryProcessor}
 * implementations, where a backup is done sending the completely changed resulting
 * object to the backup-partition, is that implementations of this sub-interface can create
 * additional {@link javax.cache.processor.EntryProcessor} instances that are sent
 * to the backup partitions to apply logic which is either different from the owner
 * partition (e.g. not sending emails) or is the simple case of being similar to the main
 * operations. In the later case {@link #createBackupEntryProcessor()} can also return
 * <pre>this</pre>.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 * @param <T> the type of the return value
 * @see javax.cache.processor.EntryProcessor
 * @since 3.4
 */
public interface BackupAwareEntryProcessor<K, V, T>
        extends EntryProcessor<K, V, T> {

    /**
     * Either creates a new, specialized {@link javax.cache.processor.EntryProcessor}
     * to be executed on the backup-partition, or returns <pre>this</pre> to execute
     * the same processor remotely.
     * <p>
     * If null is returned, the value is backed up using the normal value backup
     * mechanism, no exception is thrown, and the update is applied as expected.
     *
     * @return the backup-partition EntryProcessor
     */
    EntryProcessor<K, V, T> createBackupEntryProcessor();

}
