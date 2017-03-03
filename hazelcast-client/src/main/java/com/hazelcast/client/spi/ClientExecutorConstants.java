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

package com.hazelcast.client.spi;

import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Client Execution service executor names
 */
public final class ClientExecutorConstants {

    /**
     * INTERNAL_EXECUTOR_POOL_SIZE
     */
    public static final HazelcastProperty INTERNAL_EXECUTOR_POOL_SIZE
            = new HazelcastProperty("hazelcast.client.internal.executor.pool.size", 3);

    /**
     * name of the cluster listener executor
     */
    public static final String CLUSTER_EXECUTOR = "hz:client:cluster";

    /**
     * name of the internal executor
     */
    public static final String INTERNAL_EXECUTOR = "hz:client:internal";

    /**
     * name of the user configured executor
     */
    public static final String USER_EXECUTOR = "hz:client:user";

    /**
     * name of the listener registration executor
     */
    public static final String REGISTRATION_EXECUTOR = "hz:client:registrationExecutor";

    /**
     * name of the event executor
     */
    public static final String EVENT_EXECUTOR = "hz:client:event";

    private ClientExecutorConstants() {

    }

}
