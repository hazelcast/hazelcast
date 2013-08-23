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

/**
 * @author mdogan 6/17/13
 */
public interface PartitionView {

    static final int MAX_REPLICA_COUNT = 7;
    static final int MAX_BACKUP_COUNT = MAX_REPLICA_COUNT - 1;

    int getPartitionId();

    Address getOwner();

    Address getReplicaAddress(int index);

    boolean isBackup(Address address);

    boolean isOwnerOrBackup(Address address);

    int getReplicaIndexOf(Address address);
}
