/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;

/**
 * {@link LoadBalancer} allows you to send operations to one of a number of endpoints(Members).
 * It is up to the implementation to use different load balancing policies.
 * <p>
 * If Client is configured with {@link ClientNetworkConfig#isSmartRouting()},
 * only the operations that are not key based will be routed to the endpoint returned by the LoadBalancer. If it is
 * not {@link ClientNetworkConfig#isSmartRouting()}, {@link LoadBalancer} will not be used.
 * <p>
 *
 * For configuration see  {@link com.hazelcast.client.config.ClientConfig#setLoadBalancer(LoadBalancer)}
 */
public interface LoadBalancer {

    /**
     * Initializes the LoadBalancer.
     *
     * @param cluster the Cluster this LoadBalancer uses to select members from.
     * @param config  the ClientConfig.
     */
    void init(Cluster cluster, ClientConfig config);

    /**
     * Returns the next member to route to.
     *
     * @return Returns the next member or null if no member is available
     */
    Member next();

    /**
     * Returns the next data member to route to.
     *
     * @return Returns the next data member or null if no data member is available
     * @throws UnsupportedOperationException if the operation is not supported by this instance
     */
    default Member nextDataMember() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns whether this instance supports getting data members through a call to {@link #nextDataMember()}.
     * <p>
     * This method is used by components that require communication with data members only, such as the SQL engine.
     *
     * @return {@code true} if this instance supports getting data members through a call to {@link #nextDataMember()}
     * @see #nextDataMember()
     */
    default boolean canGetNextDataMember() {
        return false;
    }
}
