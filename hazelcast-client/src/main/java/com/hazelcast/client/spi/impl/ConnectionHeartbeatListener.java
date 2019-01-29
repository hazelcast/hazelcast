/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.nio.Connection;

/**
 * A listener for the {@link com.hazelcast.client.connection.ClientConnectionManager} to listen to connection heartbeats.
 */
public interface ConnectionHeartbeatListener {

    /**
     * This event will be fired when the heartbeat is resumed for a connection to a member.
     */
    void heartbeatResumed(Connection connection);

    /**
     * This event will be fired when no heartbeat response is received for
     * {@link com.hazelcast.client.spi.properties.ClientProperty#HEARTBEAT_TIMEOUT} milliseconds from the member.
     */
    void heartbeatStopped(Connection connection);
}
