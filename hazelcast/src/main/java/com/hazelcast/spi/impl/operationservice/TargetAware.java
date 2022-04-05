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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.cluster.Address;

/**
 * An {@link Operation} that has to be aware of the remote
 * target member address before being serialized and sent over the network
 * can implement {@code TargetAware} interface so the target {@link Address}
 * will be injected to it.
 * <p>
 * The target {@link Address} injection only happens when the
 * {@link Operation} is invoked using the invocation system.
 * When a {@code TargetAware} operation is explicitly sent to a remote target
 * or executed locally, {@link #setTarget(Address)} should be explicitly called
 * before the operation is sent or executed.
 */
public interface TargetAware {

    /**
     * Provides the designated target member address to which the operation
     * will be sent.
     * <ul>
     *     <li>
     *         This method is invoked on the implementing object before the operation
     *         is serialized and sent over the network
     *     </li>
     *     <li>
     *         When an operation is retried, the target may be re-evaluated (eg a
     *         partition operation may target a different member after a topology
     *         change). {@code setTarget} will be invoked on each retry with the
     *         current target member's {@code Address} as argument
     *     </li>
     *     <li>
     *         When an operation is executed on the local member, the local member's
     *         {@code address} is provided as target address
     *     </li>
     * </ul>
     *
     * @param address target member's address
     */
    void setTarget(Address address);
}
