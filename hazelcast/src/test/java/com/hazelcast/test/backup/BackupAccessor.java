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

package com.hazelcast.test.backup;

/**
 * Accesses backup records in a given replica.
 * <p>
 * All accessors use a partition thread to access values data hence
 * they are safe to use with HD-backed data structures.
 * <p>
 * Use {@link TestBackupUtils} to create an accessor instance for
 * your data structure.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface BackupAccessor<K, V> {

    /**
     * Number of existing backup entries in a given structure and replica index.
     *
     * @return number of backup entries
     */
    int size();

    /**
     * Returns the backup value for the given key.
     *
     * @param key key of the backup entry to get
     * @return backup value or {@code null}
     */
    V get(K key);
}
