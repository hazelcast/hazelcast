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

import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.version.Version;

/**
 * An UnsupportedClusterVersionException is thrown when a Hazelcast Client
 * attempts to connect to a cluster with a configuration that is not supported
 * by the cluster version.
 * <p>
 * For example, a client configured with routing mode :
 * {@link RoutingMode#SUBSET}
 * and RoutingStrategy
 * {@link RoutingStrategy#PARTITION_GROUPS}
 * will be unable to connect to a cluster that has a minimum cluster version of
 * less than the supported version of {@link Version#V5_5}.
 */
public class UnsupportedClusterVersionException extends HazelcastException {
    /**
     * Creates an UnsupportedClusterVersionException with the given message.
     *
     * @param message the message for the exception
     */
    public UnsupportedClusterVersionException(String message) {
        super(message);
    }
}
