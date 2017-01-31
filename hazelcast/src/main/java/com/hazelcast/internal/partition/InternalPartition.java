/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartition;

public interface InternalPartition extends IPartition {

    int MAX_REPLICA_COUNT = MAX_BACKUP_COUNT + 1;

    /**
     * Return the replica index for this partition.
     *
     * @param address the replica address
     * @return the replica index or -1 if the address is null or the address is not in the replica list
     */
    int getReplicaIndex(Address address);
}
