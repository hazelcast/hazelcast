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

package com.hazelcast.partition;

import com.hazelcast.core.MigrationListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CoreService;

import java.util.List;
import java.util.Map;

/**
 * @author mdogan 1/24/13
 */
public interface PartitionService extends CoreService {

    /**
     *
     * @param partitionId
     * @return
     */
    Address getPartitionOwner(int partitionId);

    /**
     * Returns the PartitionView for a given partitionId.
     *
     * The PartitionView for a given partitionId wil never change; so it can be cached safely.
     *
     * @param partitionId the partitionId
     * @return the PartitionView.
     */
    PartitionView getPartition(int partitionId);

    /**
     * Returns the partition id for a Data key.
     *
     * @param key the Data key.
     * @return the partition id.
     * @throws java.lang.NullPointerException if key is null.
     */
    int getPartitionId(Data key);

    /**
     * Returns the partition id for a given object.
     *
     * @param key the object key.
     * @return the partition id.
     */
    int getPartitionId(Object key);

    /**
     * Returns the number of partitions.
     *
     * @return the number of partitions.
     */
    int getPartitionCount();

    /**
     * Checks if there currently are any migrations.
     *
     * @return true if there are migrations, false otherwise.
     */
    boolean hasOnGoingMigration();

    List<Integer> getMemberPartitions(Address target);

    Map<Address, List<Integer>> getMemberPartitionsMap();

    int getMemberGroupsSize();

    String addMigrationListener(MigrationListener migrationListener);

    boolean removeMigrationListener(String registrationId);
}
