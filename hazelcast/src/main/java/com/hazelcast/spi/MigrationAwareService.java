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

package com.hazelcast.spi;

/**
 * An interface that can be implemented by SPI services to get notified of partition changes. E.g. if a
 * {@link com.hazelcast.map.MapService} that start moving its data around because partitions are moving
 * to a different machine.
 */
public interface MigrationAwareService {

    Operation prepareReplicationOperation(PartitionReplicationEvent event);

    void beforeMigration(PartitionMigrationEvent event);

    void commitMigration(PartitionMigrationEvent event);

    void rollbackMigration(PartitionMigrationEvent event);

    void clearPartitionReplica(int partitionId);
}
