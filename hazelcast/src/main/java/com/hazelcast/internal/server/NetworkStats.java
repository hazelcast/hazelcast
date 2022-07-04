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

package com.hazelcast.internal.server;

/**
 * Stats per {@link ServerConnectionManager} for both directions of network traffic (inbound or outbound).
 * <p>
 * Stores number of bytes sent and received. Used only when Advanced Networking is enabled.
 */
public interface NetworkStats {

    /**
     * Returns number of bytes received over all connections (active and closed) managed by
     * the EndpointManager. Guaranteed to be monotonically increasing counter, but
     * may not show the latest total.
     *
     * @return number of received bytes
     */
    long getBytesReceived();

    /**
     * Returns number of bytes sent over all connections (active and closed) managed by
     * the EndpointManager. Guaranteed to be monotonically increasing counter, but
     * may not show the latest total.
     *
     * @return number of sent bytes
     */
    long getBytesSent();

}
