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

package com.hazelcast.crdt;

import com.hazelcast.spi.exception.RetryableHazelcastException;

/**
 * Exception that indicates that the receiver of a CRDT operation is not
 * a CRDT replica.
 * This may happen if the membership lists of the operation sender and
 * receiver are (temporarily) different and the sender and receiver do
 * not agree on which members are the CRDT replicas.
 *
 * @since 3.10
 */
public class TargetNotReplicaException extends RetryableHazelcastException {
    public TargetNotReplicaException(String message) {
        super(message);
    }
}
