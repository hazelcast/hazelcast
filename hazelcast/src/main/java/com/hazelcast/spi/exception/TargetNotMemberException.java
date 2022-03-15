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

/**
 * A {@link com.hazelcast.spi.exception.RetryableHazelcastException} that indicates operation was sent to a
 * machine that isn't member of the cluster.
 */
public class TargetNotMemberException extends RetryableHazelcastException {

    // RU_COMPAT
    private static final long serialVersionUID = -3791433456807089118L;

    public TargetNotMemberException(String message) {
        super(message);
    }

    public TargetNotMemberException(Object target, int partitionId, String operationName, String serviceName) {
        super("Not Member! target: " + target + ", partitionId: " + partitionId
                + ", operation: " + operationName + ", service: " + serviceName);
    }
}
