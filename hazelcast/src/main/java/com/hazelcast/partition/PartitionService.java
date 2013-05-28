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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CoreService;

import java.util.List;
import java.util.Map;

/**
 * @mdogan 1/24/13
 */
public interface PartitionService extends CoreService {

    Address getPartitionOwner(int partitionId);

    boolean isPartitionMigrating(int partitionId);

    PartitionInfo getPartitionInfo(int partitionId);

    int getPartitionId(Data key);

    int getPartitionId(Object key);

    int getPartitionCount();

    int getStateVersion();

    boolean hasOnGoingMigration();

    List<Integer> getMemberPartitions(Address target);

    Map<Address, List<Integer>> getMemberPartitionsMap();
}
