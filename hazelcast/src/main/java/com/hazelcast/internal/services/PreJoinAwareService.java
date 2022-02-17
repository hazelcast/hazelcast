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

package com.hazelcast.internal.services;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

/**
 * Services which need to perform operations on a joining member just before the member is set as joined must implement
 * this interface. These operations are executed on the joining member before it is set as joined, in contrast to
 * {@link PostJoinAwareService#getPostJoinOperation()}s which are executed on the joining member after it is set as joined.
 * The practical outcome is that pre-join operations are already executed before the {@link com.hazelcast.core.HazelcastInstance}
 * is returned to the caller, while post-join operations may be still executing.
 *
 * @since 3.9
 */
public interface PreJoinAwareService {

    /**
     * An operation to be executed on the joining member before it is set as joined. As is the case with
     * {@link PostJoinAwareService#getPostJoinOperation()}s, no partition locks, no key-based locks, no service level
     * locks, no database interaction are allowed. Additionally, a pre-join operation is executed while the cluster
     * lock is being held on the joining member, so it is important that the operation finishes quickly and does not
     * interact with other locks.
     *
     * The {@link Operation#getPartitionId()} method should return a negative value.
     * This means that the operations should not implement {@link PartitionAwareOperation}.
     * <p>
     * Pre join operations should return response, which may also be a {@code null} response.
     *
     * @return an operation to be executed on joining member before it is set as joined. Can be {@code null}.
     */
    Operation getPreJoinOperation();
}
