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

package com.hazelcast.wan;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;

/**
 * Interface to be implemented by custom WAN event consumers. Wan replication
 * consumers are typically used in conjunction with a custom
 * {@link WanPublisher}. The publisher will then publish events
 * in a custom fashion which the consumer expects and processes accordingly.
 * This way, you can provide custom publication and consumption mechanisms
 * and protocols for WAN replication. The default implementation of the WAN
 * publisher ignores any configured custom consumers and the WAN events will
 * be processed as if there was no custom consumer implementation.
 * Can be registered by programmatically or by XML using {@link WanConsumerConfig}.
 *
 * @see WanPublisher
 */
public interface WanConsumer {

    /**
     * Initialize the WAN consumer. The method is invoked once the node has
     * started.
     * The WAN consumer is responsible for registering itself for WAN event
     * consumption. Typically this means that you would either use the
     * {@link com.hazelcast.spi.impl.executionservice.ExecutionService}
     * to schedule the consumer to run periodically or continually by having an
     * implementation which uses blocking or spinning locks to check for new
     * events. The implementation is free however to choose another approach.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param config             the WAN consumer config
     */
    void init(String wanReplicationName, WanConsumerConfig config);

    /**
     * Callback method to shutdown the WAN replication consumer. This is called
     * on node shutdown.
     */
    void shutdown();
}
