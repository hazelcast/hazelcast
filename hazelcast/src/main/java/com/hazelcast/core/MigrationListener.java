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

import java.util.EventListener;

/**
 * MigrationListener provides the ability to listen to partition migration events.
 *
 * @see Partition
 * @see PartitionService
 */
public interface MigrationListener extends EventListener {

    /**
     * Invoked when a partition migration is started.
     *
     * @param migrationEvent event
     */
    void migrationStarted(MigrationEvent migrationEvent);

    /**
     * Invoked when a partition migration is completed.
     *
     * @param migrationEvent event
     */
    void migrationCompleted(MigrationEvent migrationEvent);

    /**
     * Invoked when a partition migration is failed.
     *
     * @param migrationEvent event
     */
    void migrationFailed(MigrationEvent migrationEvent);
}
