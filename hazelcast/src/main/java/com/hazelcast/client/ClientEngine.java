/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ProxyService;

/**
 * The client Engine.
 *
 * todo: what is the purpose of the client engine.
 */
public interface ClientEngine {

    int getClientEndpointCount();

    InternalPartitionService getPartitionService();

    ClusterService getClusterService();

    EventService getEventService();

    ProxyService getProxyService();

    Config getConfig();

    ILogger getLogger(Class clazz);

    Address getMasterAddress();

    Address getThisAddress();

    MemberImpl getLocalMember();

    SecurityContext getSecurityContext();
}
