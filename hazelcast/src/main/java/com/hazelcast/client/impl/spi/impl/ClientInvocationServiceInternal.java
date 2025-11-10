/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.sequence.CallIdSequence;

import java.io.IOException;

public interface ClientInvocationServiceInternal extends ClientInvocationService {


    /**
     * Get invocation logger
     *
     * @return class logger
     */
    ILogger getInvocationLogger();

    /**
     * Get invocation timeout
     *
     * @return invocation timeout
     */
    long getInvocationTimeoutMillis();

    /**
     * Interval for retry attempts in invoking.
     *
     * @return invocation retry Interval
     */
    long getInvocationRetryPauseMillis();


    /**
     * Get call id sequence.
     *
     * @return call id sequence
     */
    CallIdSequence getCallIdSequence();


    /**
     * remove invocation
     *
     * @param callId invocation id
     */
    void deRegisterInvocation(long callId);

    /**
     * Check the connected state and user connection strategy configuration to see
     * if an invocation is allowed at the moment
     * returns without throwing exception only when is the client is Connected to cluster
     */
    void checkInvocationAllowed() throws IOException;

    /**
     * Check the connected state and user connection strategy configuration to see
     * if an urgent invocation is allowed at the moment
     */
    void checkUrgentInvocationAllowed(ClientInvocation invocation);

    /**
     * Returns {@code true} if the configuration for failing on an indeterminate operation state is enabled,
     * {@code false} otherwise.
     */
    boolean shouldFailOnIndeterminateOperationState();

    /**
     * @return returns the configured routing mode(by default it is {@link RoutingMode#ALL_MEMBERS})
     * @see RoutingMode
     */
    RoutingMode getRoutingMode();
}
