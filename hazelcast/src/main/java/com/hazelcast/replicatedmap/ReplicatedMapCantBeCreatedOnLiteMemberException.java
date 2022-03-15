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

package com.hazelcast.replicatedmap;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cluster.Address;

/**
 * Thrown when {@link com.hazelcast.core.HazelcastInstance#getReplicatedMap(String)} is invoked on a lite member.
 */
public class ReplicatedMapCantBeCreatedOnLiteMemberException extends HazelcastException {

    public ReplicatedMapCantBeCreatedOnLiteMemberException(Address address) {
        this("Can't create replicated map instance on " + address);
    }

    public ReplicatedMapCantBeCreatedOnLiteMemberException(String message) {
        super(message);
    }
}
