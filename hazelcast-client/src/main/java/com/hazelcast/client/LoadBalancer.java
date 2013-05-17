/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;

/**
 *
 * {@link LoadBalancer} allows you to send operations to one of a number of endpoints(Members).
 * It is up to the implementation to use different load balancing policies. If Client is {@link ClientConfig#smart},
 * only the operations that are not key based will be router to the endpoint returned by the Load Balancer.
 * If it is not {@link ClientConfig#smart}, {@link LoadBalancer} will be used for all operations.
 */
public interface LoadBalancer {

    public void init(Cluster cluster, ClientConfig config);

    /**
     * Returns the next member to route to
     * @return Returns the next member or null if no member is available
     */
    public Member next();
}
