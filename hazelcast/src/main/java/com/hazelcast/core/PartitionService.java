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

package com.hazelcast.core;

import java.util.Set;

/**
 * PartitionService allows to query {@link Partition}s
 * and attach/detach {@link MigrationListener}s to listen partition migration events.
 *
 * @see Partition
 * @see MigrationListener
 */
public interface PartitionService {

    /**
     * Returns all partitions.
     *
     * @return all partitions
     */
    Set<Partition> getPartitions();

    /**
     * Returns partition which given key belongs to.
     *
     * @param key key
     * @return partition which given key belongs to
     */
    Partition getPartition(Object key);

    /**
     * Generates a random partition key. This is useful if you want to partition data in the same partition,
     * but don't care which partition it is going to be.
     *
     * The returned value will never be null.
     *
     * @return the random partition key.
     */
    String randomPartitionKey();

    /**
     * @param migrationListener listener
     * @return returns registration id.
     */
    String addMigrationListener(MigrationListener migrationListener);

    /**
     * @param registrationId Id of listener registration.
     * @return true if registration is removed, false otherwise
     */
    boolean removeMigrationListener(final String registrationId);

}
