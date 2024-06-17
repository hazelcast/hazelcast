/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.core.HazelcastException;

/**
 * An UnsupportedRoutingModeException is thrown when a Hazelcast Client
 * is configured with a {@link RoutingMode} that is not supported by the cluster.
 * <p>
 * For example, a client configured with routing mode :
 * {@link RoutingMode#SUBSET}
 * and RoutingStrategy
 * {@link RoutingStrategy#PARTITION_GROUPS}
 * will be unable to connect to a cluster that is not correctly licensed
 * to send partition group info.
 */
public class UnsupportedRoutingModeException extends HazelcastException {

    /**
     * Creates an UnsupportedRoutingModeException with the given message.
     *
     * @param message the message for the exception
     */
    public UnsupportedRoutingModeException(String message) {
        super(message);
    }
}
