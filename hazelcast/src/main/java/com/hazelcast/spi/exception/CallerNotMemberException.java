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
 * A {@link RetryableHazelcastException} that indicates that an operation was sent by a machine which isn't member
 * in the cluster when the operation is executed.
 */
public class CallerNotMemberException extends RetryableHazelcastException {

    public CallerNotMemberException(Address thisAddress, Address caller, int partitionId,
            String operationName, String serviceName) {
        super("Not Member! this: " + thisAddress + ", caller: " + caller + ", partitionId: " + partitionId
                + ", operation: " + operationName + ", service: " + serviceName);
    }

    public CallerNotMemberException(String message) {
        super(message);
    }
}
