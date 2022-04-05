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

import com.hazelcast.cluster.Member;

/**
 * A {@link com.hazelcast.spi.exception.RetryableHazelcastException} indicating that an operation is executed on
 * a wrong member.
 */
public class WrongTargetException extends RetryableHazelcastException {

    // RU_COMPAT
    private static final long serialVersionUID = -84600702836709317L;

    private transient Member target;

    public WrongTargetException(Member localMember, Member target, int partitionId, int replicaIndex, String operationName) {
        this(localMember, target, partitionId, replicaIndex, operationName, null);
    }

    public WrongTargetException(Member localMember, Member target, int partitionId, int replicaIndex,
                                String operationName, String serviceName) {
        super("WrongTarget! local: " + localMember + ", expected-target: " + target
                + ", partitionId: " + partitionId + ", replicaIndex: " + replicaIndex
                + ", operation: " + operationName + ", service: " + serviceName);

        this.target = target;
    }

    public WrongTargetException(String message) {
        super(message);
    }

    public Member getTarget() {
        return target;
    }
}
