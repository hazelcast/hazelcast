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

package com.hazelcast.spi.exception;

import com.hazelcast.cluster.Address;

/**
 * A {@link com.hazelcast.spi.exception.RetryableHazelcastException} that is thrown when an operation is executed
 * on a partition, but that partition is currently being moved around.
 */
public class PartitionMigratingException extends RetryableHazelcastException {

    public PartitionMigratingException(Address thisAddress, int partitionId, String operationName, String serviceName) {
        super("Partition is migrating! this: " + thisAddress + ", partitionId: " + partitionId
                + ", operation: " + operationName + ", service: " + serviceName);
    }

    public PartitionMigratingException(String message) {
        super(message);
    }
}
